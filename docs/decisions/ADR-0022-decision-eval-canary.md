# ADR-0022 — Decision-opportunity eval, replay, one-kind canary and accounting

Status: accepted (J5, base e2b40f4). Implements `IMPLEMENTATION_PLAN.md` §J5 and
`EVAL_PLAN.md`. Builds on ADR-0018 (chain/ledger), ADR-0020 (intake shadow) and
ADR-0021 (planning tier).

## 1. Decision-opportunity corpus

`experiments/decision_corpus.py` defines a corpus whose unit of observation is a
pre-decision opportunity (current turn, snapshot ref, allowed effects/refs) with
**human** labels, separate from the bench `corpus.py`. The temporal-leakage
guard requires every `group_id` (conversation/project/time cluster, including
synthetic paraphrases) to live in exactly one split. The seed fixture holds two
cases per family across the eight families; it is an assembly minimum, not a
statistical gate.

## 2. Replay explains each layer

`experiments/replay.py` replays one fixed recorded request and reports parsing
(strict `decision.v3` validator), mapping (label -> advisory route hint) and
application (advisory-only or a caller-supplied effect) as separate stages, so
an outcome is attributed to the right layer.

## 3. One-kind deterministic canary

`experiments/canary.py` admits exactly one allowlisted kind in a stable
hash-bucket cohort (`cohort_bucket`, `in_cohort`). Admission requires the kind to
be the single enabled kind, the `WhitelistGate` to allow it, the per-kind
`KillSwitch` to be off, and the opportunity to fall in the cohort. Nothing is
based on model self-confidence. `rollback()` freezes the canary kind and returns
the previous limited path. `canary_policy_from_settings` reads the new
`Settings.jev_enabled_kinds` / `jev_canary_percent` / `jev_kill_switch_kinds`
fields (default off).

## 4. Pre-registered thresholds and per-family metrics

`experiments/decision_eval.py` declares `EvalThresholds` and
`DEFAULT_THRESHOLDS` in-module *before* any arm is chosen. `evaluate_traces`
computes per-family and overall coverage/target-accuracy/false-execute/unauthorized
metrics and fails the report when a pre-registered threshold is missed. An
effect applied outside the admitted cohort counts as an unauthorized transition.

## 5. Full-cost accounting and late results

`load_decision_accounting` folds the existing usage ledger
(`usage_totals_by_purpose` + `usage_retry_subtotal`), so retries/fallbacks are
part of the total call count and cost, not just the first call. A late result
after the kill switch is recorded through the J1 `record_late_decision` Event
path and never changes Task/Run/WorkPackage state (verified by an integration
test).

## Consequences

- J6 must not start without residual-error evidence from this harness.
- The seed corpus is explicitly not enough for rollout; expansion requires the
  statistical gate in `EVAL_PLAN.md` (denominator and uncertainty reported).
