# Procedure runbook: authoring, promotion, revocation, rollback (WP10)

## 1. Authoring guide

Procedures compose existing tools; they never install anything themselves
(the installer stays the backend). Author in code, over the WP03 registry:

```python
from vuzol.projects.procedures import ProcedureDescriptor, ProcedureStage, ProcedureStep

descriptor = ProcedureDescriptor(
    procedure_id="repo.quality",
    version="1",
    label="Repository quality",
    stages=(
        ProcedureStep(stage=ProcedureStage.ENVIRONMENT, action="installation_states", version="installations.v1"),
        ProcedureStep(stage=ProcedureStage.GATES, action="trusted_gates", version="gates.v1"),
        ProcedureStep(stage=ProcedureStage.REPORT, action="quality_report", version="report.v1"),
    ),
    requires=("git", "python-runtime"),
    run_pins=("environment_hash",),
)
```

Rules:

- `procedure_id@version` is immutable once promoted; a new version is a new
  ref (`repo.quality@2`), never an edit. In-flight runs keep resolving the
  pinned ref (same guarantee as toolchain run pins: drift fails closed).
- Stage actions must reference existing tools/steps (`installation_states`,
  `resolve_trusted_gates`, report builders). No plugin loader, no remote
  nodes, no embedded interpreters.
- `requires` lists registry descriptor keys; `run_pins` lists the environment
  facts pinned at call time (`environment_hash`).

## 2. Sample: repo.quality@1

- Ref: `repo.quality@1` (`src/vuzol/projects/procedures.py:repo_quality_procedure`).
- Stage A (environment): healthy installation required —
  `installation_states()` must report the required capabilities as
  `installed` (fresh `health_until`); `stale`/`failed` stop the procedure
  before any gate runs.
- Stage B (gates): declared gates only — `resolve_trusted_gates` allowlist
  (8 trusted commands) plus mandatory `secret-scan`; unknown commands fail
  closed; a failed gate blocks the commit but retains measured evidence.
- Stage C (report/logs): gate payloads (`_success_payload` shape) plus the
  procedure receipt → `Artifact(procedure_receipt, application/json)` with
  `receipt_hash == content_hash` by construction (`require_receipt_link`
  re-verifies on read).

## 3. Draft / promotion / revocation policy (minimal)

- **Draft**: `DraftStore.save_draft(descriptor, author=...)` — visible only
  to its author (`draft_for(ref, author=...)` returns None for anyone else);
  drafts never appear in `ProcedureRegistry.lookup`/`list_promoted`, so other
  tasks cannot resolve them.
- **Promote**: explicit `ProcedureRegistry.promote(descriptor,
  approval=...)` — the envelope must hash-bind this exact `procedure_ref` +
  `descriptor_hash` to an approver (`approve_procedure`, issuance under the
  current policy on the caller's side); forged/mismatched approvals raise
  `ProcedureApprovalMismatch` and the procedure never resolves. No
  auto-promotion.
- **Revoke**: explicit `ProcedureRegistry.revoke(ref)` — the ref stops
  resolving immediately and returns the capability keys the caller must
  quarantine (failed-probe exclusion via `record_installation`); already-
  published receipts/artifacts stay readable (evidence is append-only).
- **Failed probe → quarantine by exclusion**: `record_installation` with a
  failed probe stores `status=failed`; `installation_states` reports `failed`,
  never `installed`, so stage A excludes it from selection. Revocation of
  grants additionally flows through the WP05 permission check.

## 3b. Execution (stages over existing ports)

`src/vuzol/projects/runner.py:run_procedure` — lookup (promoted only) →
stage A (all `requires` healthy in `installation_states`, else
`environment_not_ready` before any gate runs) → stage B (gate ids resolved
against the `TRUSTED_GATE_COMMANDS` allowlist, unknown rejected; every gate
runs, results collected) → stage C (receipt built; any failed gate raises
`ProcedureRunFailed` carrying the receipt as evidence). `publish_receipt_artifact`
persists through the WP02 artifact contract. Gate execution itself is an
injected port (`GateRunner`); `LocalGateRunner` runs allowlisted commands
locally with a timeout.

## 4. Rollback

Remove the descriptor from the promoted registry (`revoke`). Installations,
receipts, approvals, and published bundles remain untouched and readable;
`coding.v3/v4` steps are unchanged, so no flag is needed. Re-promotion is a
new explicit promote (with a fresh approval if source/hash changed — the
existing changed-envelope rule).

## 5. Reuse measurement

The second caller resolves the same promoted ref with zero new setup:

- `registry.lookup("repo.quality@1")` returns the identical descriptor
  (`descriptor_hash` equal) — unit-covered by
  `test_second_lookup_reuses_procedure_without_new_setup`.
- Environment reuse is measured, not assumed: stage A re-checks
  `installation_states` on every call and compares the pinned
  `environment_hash` before any gate runs; a toolchain change under a pinned
  run raises `CapabilityPinMismatch` (via `run_pin_mismatch`, same fail-closed
  contract as `enforce_run_pins`) instead of silently re-resolving.

## 6. Cleanup

Only owned resources: installation roots under the provisioning roots and
the procedure's own artifacts. Foreign paths are never touched; quarantine
moves follow the existing retention mechanism (contained, symlink-refused).
