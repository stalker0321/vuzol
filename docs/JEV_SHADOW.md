# Jev in shadow (WP11)

Cheap repair-triage decisions observed without touching execution. One
decision class `repair_triage`, finite choices plus abstain, labelled
fixture corpus, shadow records in the existing `Event` ledger. No live
model benchmark (no budget) — fixtures and deterministic tests only.

Lead decisions (binding): no model-derived grants and no auto-promotion
(anywhere, including docs); shadow never changes execution
(`workflows/transitions.py`, `workflows/service.py` untouched); abstain
without probabilities routes to the existing policy path; schema lives in
`docs/schemas/decision.v1.schema.json`.

## 1. decision.v1

Contract: `docs/schemas/decision.v1.schema.json`, implemented by pure
`src/vuzol/experiments/decision.py` (`DECISION_SCHEMA = "decision.v1"`).

Fields: `schema`, `decision_kind` (`repair_triage` const), `state_revision`
(int), `choice` (`retry | repair | wait | attention`), `evidence_refs`
(`artifact:<type>:sha256:<hex>`, re-verifiable against retained bytes),
`reason_code` (finite allowlist of 11), `abstain` (bool),
`input_fingerprint` (sha256 of the full input: rubric/model/prompt/policy
versions + state + evidence revisions — the cache key unit of report §9).

Fail-closed rules (report §9):

- old answer / wrong `state_revision` → `DecisionStale`, no transition;
- invalid schema / unsupported choice / missing evidence → no transition;
- at most one schema-repair pass (trim + drop unknown keys), then abstain;
- evidence link re-checked on read (`verify_evidence_link`), mismatch fails;
- abstain requires `choice: attention`; non-abstain requires evidence refs.
- The allowlists are the injection defense: smuggled instructions cannot
  match `choice`/`reason_code`/`evidence_refs` exactly.

`authorize_execution(decision, expected_state_revision, evidence)` returns
the existing `RecoveryAction` (abstain → `ATTENTION`) or raises. There are
no grant, promotion, score or calibration fields — nothing here can open
permissions or promote anything.

## 2. Rubric (labelling rules for repair_triage)

| Failure signal | Shadow choice | Reason | Abstain? |
|---|---|---|---|
| repairable step + fresh evidence, caps open | repair | known_local_failure | no |
| transient + retryable | retry | transient_retryable | no |
| backpressure category, waits left | wait | backpressure_wait | no |
| unknown effects / deadline / caps / oscillation | attention | unknown_failure / deadline_exceeded / cap_exhausted / oscillation_guard | no |
| unseen failure class / adversarial source text | — | ood_input | **yes** |
| evidence ref missing or link broken | — | missing_evidence | **yes** |
| unrepairable output | — | invalid_output | **yes** |
| revision drift | — | stale_state | **yes** (no transition) |

Abstain carries no probabilities and no measurement of OOD — OOD is a
labelling verdict (unseen class / adversarial text), not a score.

## 3. Confusion matrix protocol

Rows = rules action (`decide_recovery`), columns = shadow choice, plus an
abstain column/row. Per holdout slice report: false-accept (shadow would
act where rules abstain), false-reject (shadow abstains where rules act),
abstention rate, coverage (share of non-abstained pairs), and downstream
cost with retries (`report_routes`: rules/cheap/strong totals, `C_success`
on totals, undefined when a route has 0 successes). Holdout must contain
adversarial source instructions and unseen failure classes
(`decision-corpus.v1.json` seeds this; full holdout per WP13 protocol).
Per-cell costs come from `usage_totals_by_purpose`; retry rows are a
projection of the same rows, never an addend.

## 4. Rollout / circuit breaker

1. Offline fixture corpus (this task).
2. Shadow records in `Event` ledger (`jev.shadow_recorded`) with execution
   bit-for-bit unchanged (pinned by transition tests).
3. Limited whitelist of decision classes — `WhitelistGate` default off;
   enabling one class requires explicit gate evidence
   (preregistration/report hash) and records it.
4. Circuit breaker: on regression (rising false-accepts, escaped defects,
   cost overrun) the class is removed from the whitelist back to default
   off; removal is immediate and needs no migration.
5. Promotion of any class to production transitions requires a separate
   lead decision with ADR/eval — never automatic, never model-derived.

`workflows/transitions.py`, `workflows/service.py`, `models.py` and
migrations are untouched by this package (zero-migration requirement).
