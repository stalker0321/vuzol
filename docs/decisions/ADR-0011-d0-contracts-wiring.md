# ADR-0011 — D0 contracts/wiring: review floors, research split, pinned admission

Status: accepted (D0 writer, base 9be9054). Implements DELTA §D0.

## 1. Review coverage/floors

- L2 and L3 require a bounded independent model call; L0/L1 are mechanical
  only (`review/policy.py:requires_independent`, `review/handler.py:_requires_independent_for`).
- MEDIUM maps to L2, so an actual medium handler invokes the required reviewer.
  Cost increase for MEDIUM is accepted (lead decision).
- HIGH/PRIVILEGED are never weakened: `effective_risk`/`runtime_risk` escalate
  only; policy resolves the max over files.
- Policy errors fail closed to `OutcomeKind.BLOCKED`
  (`independent_review_required`), never silent pass.
- `select_reviewer_profile` receives the policy level + budget/role eligibility.
  PLANNER API fallback is explicit with a log (operator without a REVIEWER
  profile gets BLOCKED when no profile exists, not silent pass). EXECUTOR tier
  is never eligible as reviewer.
- `should_skip_rereview` is documented unused by dispatch (pure helper).
- `l1_enabled` has no operator setting in D0 (promise removed from
  `policy.py`/`REVIEW_BOUNDARIES.md`); the parameter remains as an explicit
  L1→L2 escalation path only.
- No global mandatory-review flag for all task types in D0.
- Direct commands/status (`apply_task_command`, `TaskControlService`) never
  create LLM review: they do not materialize workflows or review steps.

## 2. Legacy compatibility

- Frozen WP00 contracts are immutable copies in `docs/contracts/` +
  `docs/schemas/` (base 9be9054). Contents unchanged; only tracked paths.
- Research split: `research-provider-result.v1` (legacy provider text,
  no verified label) vs `research-result.v1` (sources/claims only).
  Legacy readers keep reading the old format without a verified label.
- `research-result.v1` consumers validate raw bytes fail-closed
  (`validate_source_report_bytes` via `jsonschema.Draft202012Validator` +
  `validate_report`), never trusting `binding.schema_version`. Mismatch fails
  before provider spend (`BindingError: source_report_schema_mismatch`,
  reservation released).
- Retrieval is not connected to `research_execute` in D0 (D3 scope).
- `permission_envelope_hash = payload_hash` semantics untouched (Q2, intent
  unclear) — documented as known gap for ADR.
- `horizon_id` in accounting is a known gap, scope D3 — not fixed, not promised.
- Verdict vocabulary (`review/domain.py`), `partitions.py` coverage/overlap/caps,
  `verify_chunk_receipts`, untrusted-diff wrapper, `result_approval` semantics,
  WP02 hash/scope/freshness checks, `retrieval.py` bounds, ADR-0007 lease/fence,
  `horizon_status` mapping, memory L0/L1/L2 names — unchanged.

## 3. Admission vs active-run semantics

- `execution_contract_version` (nullable/additive) on `Run` and `WorkPackage`
  (migration `d0c0n7r4c7v1`, no renames). `Run` pins `execution-contract.v1`
  at materialization; old rows (NULL) stay readable.
- Flag `horizon.enabled` gates admission of new plans only. `sequencer.start`
  pins `horizon-v1:enabled|disabled` at admission; active packages read the
  pinned value (`pinned_horizon_enabled`), so flag off never downgrades a
  materialized workflow (no silent legacy-COMPLETED). Pre-D0 NULL rows fall
  back to the passed flag for compatibility.
- `horizon_enabled(settings)` is the single flag reader, wired at
  `sequencer.py`, `interpretation/service.py`, `telegram/controls.py`.
- `Run.workflow_version` is documented as written-not-branched in D0.
- Stale completion, late receipts, correction fencing, duplicate delivery —
  D1/D2 scope, recorded as follow-ups.
