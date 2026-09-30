# Contracts → executable consumers (D0)

Status: frozen copies, base 9be9054. Normative schemas/ADRs are immutable;
this file only maps each contract to its executable consumer or known gap.

Tracked copies:

- `docs/contracts/ADR-A01.md`, `docs/contracts/ADR-A02.md`, `docs/contracts/ADR-A03.md`
- `docs/schemas/attempt.v1.schema.json`, `docs/schemas/effect.v1.schema.json`,
  `docs/schemas/horizon.v1.schema.json`, `docs/schemas/input-binding.v1.schema.json`,
  `docs/schemas/permission-envelope.v1.schema.json`,
  `docs/schemas/research-result.v1.schema.json` (+ `research-result.v1.example.json`)

| Contract | Tracked copy | Executable consumer | Status |
|---|---|---|---|
| ADR-A01 ownership/identity | `docs/contracts/ADR-A01.md` | `Effect` intent/receipt (`execution/result_apply.py`, `execution/effect.py`), `WorkPackage` horizon fields (`storage/models.py`), `horizon_status` mapping (`discussion/horizon.py`), fence `lease_generation` | partial; docs point to tracked copy |
| A01.3 Horizon (`horizon.v1`) | `docs/schemas/horizon.v1.schema.json` | `goal_revision`/`version`/`horizon_phase` in `storage/models.py`; `"horizon.v1"` string not used in code; `horizon_id` never filled in prod (known gap, scope D3) | gap documented, not promised |
| A01.3 Attempt (`attempt.v1`) | `docs/schemas/attempt.v1.schema.json` | no `Attempt` class/table; `"attempt.v1"` absent; `AttemptKind` → `UsageRecord.attempt_kind`; `Effect.attempt_id` always NULL | agreement without carrier; D1 `WorkAttempt`, no backfill |
| A01.3 Effect (`effect.v1`) | `docs/schemas/effect.v1.schema.json` | `Effect.schema_version` default `effect.v1` (`storage/models.py`), `EFFECT_SCHEMA_VERSION` (`execution/effect.py`), mapping in `docs/EFFECT_RECONCILIATION.md` | live, only complete one of five |
| A01.3 InputBinding | `docs/schemas/input-binding.v1.schema.json` | `InputBinding` row (`storage/models.py`); `schema_name`/`schema_version` free strings, version not pinned | implementation exists, version not pinned |
| A01.3 PermissionEnvelope | `docs/schemas/permission-envelope.v1.schema.json` | no class; only `permission_envelope_hash` (`storage/models.py`); `result_apply.py` stores `payload_hash` there — same value in `payload_hash`/`permission_envelope_hash`/`approval_envelope_hash` | known gap: field does not reflect contract; semantics untouched in D0 (Q2, intent unclear), ADR follow-up |
| ADR-A02 pricing/attribution | `docs/contracts/ADR-A02.md` | `UsageRecord.pricing_revision/currency/cost_known/purpose/attempt_kind/late_receipt`, `accounting_for_profile`, review unknown floor, `review_cost_export` | mostly wired; `horizon_id` not propagated → lifetime budget not assembled (D3) |
| ADR-A03 approvals | `docs/contracts/ADR-A03.md` | `Approval` + `action_envelope_hash`, `ensure_result_approval`, `verified_envelope`, `Principal.validate` | wired; model profile never issues approval |
| `research-result.v1` (sources/claims) | `docs/schemas/research-result.v1.schema.json` | `research/report.py:validate_report` fail-closed library; typed consumer `validate_source_report_bytes` fail-closed on bytes (D0); legacy provider text uses `research-provider-result.v1` | split by name; legacy readers read old format without verified label |
| `research-provider-result.v1` (legacy provider text) | — (provider payload, not a frozen WP00 schema) | `_research_result_bytes` in `providers/handlers.py`, `Artifact(type=research_result)` + `InputBinding(schema=research-provider-result.v1)` | legacy, no verified label |
| Review policy `review-policy.v1` | `src/vuzol/review/policy.py`, `docs/REVIEW_BOUNDARIES.md` | `ResultReviewHandler._review` decides by policy level (L2+ → independent), `select_reviewer_profile` with level/role eligibility, PLANNER fallback explicit with log, EXECUTOR never | wired in D0; `should_skip_rereview` documented unused; `l1_enabled` operator promise removed (L1 always on, disable escalates to L2 in code path only) |
| Pinned `execution_contract_version` | this file + `docs/HORIZON_RUNTIME.md` §pinned | `Run.execution_contract_version` + `WorkPackage.execution_contract_version` (nullable/additive, migration); flag gates admission of new plans, active package reads pinned value | admission, no silent downgrade; old drafts/workflows/approvals readable |

Notes:

- `horizon_id` in accounting is a known gap, scope D3 — not fixed here, not promised.
- `permission_envelope_hash = payload_hash` is a known gap for ADR — semantics untouched.
- Retrieval is not connected to `research_execute` in D0 (D3 scope) — contract + fail-closed consumer only.
- Memory L0/L1/L2 (`discussion/memory.py`) are distinct from review L0–L3 — not renamed, separated here.
- Review L1 (mechanical + focused patterns) is not Jev; Jev stays unconnected (`experiments/decision.py` shadow only).
