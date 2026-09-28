# Controlled experiment harness (WP13)

Measurable outcome economics without cherry-picked examples. Live model
benchmark is forbidden (no budget) — CI runs only the fixture corpus and
deterministic tests. Live trials stay on the existing `seed_trial` path
(`vuzol-experiment seed`), never in CI.

## 1. Corpus `experiment-corpus.v1`

`src/vuzol/experiments/corpus.py`. Strata mirror EXPERIMENTS.md §2
(isolated/integration/research/data/reuse/horizon + Jev negative set);
splits dev/calibration/held-out; smoke-8 for harness repair. Fixture:
`tests/fixtures/experiments/corpus.v1.json` (12 tasks, 8 smoke, hash-pinned
via `content_hash`). Loader: `load_corpus_manifest`.

## 2. Arms with distinct documented execution paths

`src/vuzol/experiments/arms.py` (`ExperimentArm`: current/strong_solo/hybrid):

| Arm | Steps | Budget |
|---|---|---|
| current | interpret → prepare → execute → approval | strong |
| strong_solo | interpret → prepare → execute (single owner, no approval step) | efficient |
| hybrid | interpret → prepare → execute → review → approval | balanced |

`describe_execution_path(arm)` records steps/roles/budget mode;
`plan_cohort` pairs every corpus task × seed across arms (`pair_id =
<task>:seed-<n>`) and randomizes order with an explicit recorded
`shuffle_seed` (reproducible; order stored per run).

## 3. Frozen policy snapshot `experiment-policy-snapshot.v1`

`src/vuzol/experiments/snapshot.py`: profiles, pricing (revision + rates or
explicit unknown), prompt/tool versions, env, cache policy +
policy/configuration revision. Immutable frozen model + `snapshot_hash`;
pricing compared by revision before any cash comparison.

## 4. Analysis `experiment-analysis.v1` (stdlib only)

`src/vuzol/experiments/analysis.py` — pure functions, no providers/DB:

- denominator always includes failed/aborted/censored; successes count only
  when independently `verified` (proposer self-score never ground truth);
  assisted successes excluded from the autonomous rate;
- `c_success = total cost / verified successes`; 0 successes → `None` +
  `c_success_undefined`, never 0;
- paired deltas (a−b) by pair, aggregated per family; cluster bootstrap CI
  over tasks (all pairs of one task form one cluster, EXPERIMENTS.md §4)
  with fixed seed; deadline overrun censors claimed success;
- `compare_arms` takes the declared `metric` (`arm_a` is the candidate,
  `arm_b` the control) and gates on its uncertainty: success_rate needs a
  success-delta CI strictly above 0; c_success needs a cost-ratio
  (C_a/C_b) CI strictly below 1.0 with censored/failed costs kept in the
  numerator; latency needs a duration-delta CI strictly below 0 over
  completed pairs with zero censored pairs (censored durations are lower
  bounds, not measurements). Other metrics gate on success
  non-inferiority; an unknown metric forces inconclusive (fail-closed).
  Returns `inconclusive: true` (never victory) on thin evidence: small
  paired_n, undefined C_success, pricing drift, or a metric gate missed;
- `HYPOTHESES` H1–H9 mirror report §29 (arms + metric + EX-link);
  `analyze_hypothesis` runs only the preregistered comparison;
- `classify_telemetry_outcome` maps harness `ReviewOutcome` to analysis
  status without changing the frozen taxonomy: unverified accepts →
  censored, takeover/discarded → failed, blocked_* → censored.
  (`ReviewOutcome` itself unchanged — no contract change.)

Fixture: `tests/fixtures/experiments/smoke-outcomes.v1.json` (smoke-8 × 3
arms, mixed success/failed/aborted/censored/unknown-cost).

## 5. Joint export `experiment-joint-export.v1`

`src/vuzol/experiments/export.py::joint_export`: harness summary +
`usage_totals_by_purpose` breakdown + `usage_retry_subtotal` kept as a
separate projection ("same rows / never an addend"); a consistency check
rejects retry > purpose totals. `pricing_comparable` gates cash comparison
on a single known pricing revision. `aggregate_trials` no longer folds
unknown into `or 0`: measured totals stay `None` when unmeasured, with
`*_complete` flags and explicit unavailable counts next to them.

## 6. CLI (local, no live)

- `vuzol-experiment plan CORPUS --arms … --seeds … --shuffle-seed N
  --smoke-only --json plan.json` → `experiment-run-plan.v1`.
- `vuzol-experiment analyze TRIALS.json --ledger LEDGER.json --hypotheses H1
  --json report.json` → analysis report, optionally wrapped in joint export.

## 7. Preregistration template

Before a cohort, freeze and store alongside the snapshot hash:

```yaml
hypothesis: H1  # one of H1..H9, question fixed in analysis.HYPOTHESES
arms: [hybrid, strong_solo]
corpus_revision: corpus.v1
corpus_hash: <sha256>
snapshot_hash: <sha256>
seeds: [1, 2, 3]
shuffle_seed: 7
non_inferiority_margin_quality: <pre-agreed>
desired_cost_reduction: 0.30  # product goal, not a finding
latency_target_ms: <per task contract>
stopping_rules: <max money/quota, max runs>
success_metric: c_success
```

## 8. Experimental report template

`build_report` output (`experiment-analysis.v1`): per-arm `n`,
success/autonomous rates, mean/completed/censored durations, measured vs
unknown cost, `c_success` or `undefined`; per-hypothesis comparison with
paired deltas, success/cost/latency CIs, pricing check and `inconclusive` +
reasons. `inconclusive: false` only licenses reporting the measured effect
with its uncertainty — never an architectural victory claim (that needs the
preregistered margins: quality non-inferiority, cost reduction, latency
target). A report claiming victory with `inconclusive: true`, mixed pricing
revisions, or unverified successes is invalid.

## 9. Boundaries

No live benchmark, no `telegram/dogfood.py` trigger changes, no contract /
acceptance / permission changes, no new capabilities, no drills (WP14), no
`ReviewOutcome` change, no migrations. Live trial readiness = `seed` +
`record` + `inspect` + `export` path unchanged; CI gates only fixtures.
