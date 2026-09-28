# Review boundaries (WP07)

Large valid results must not get stuck: versioned L0/L1/L2/L3 policy,
deterministic partition manifest, bounded model review per partition plus a
cross-partition assessment, and a total review cap. Risk only escalates, never
downgrades — no high-risk gate is lowered without ADR/eval. Jev is not
connected to review.

## 1. Policy `review-policy.v1`

Implemented in `src/vuzol/review/policy.py` (`REVIEW_POLICY_REVISION`).

| Level | Meaning | When |
|---|---|---|
| L0 | mechanical only | LOW risk, docs-only |
| L1 | mechanical + focused patterns; may be operator-disabled (escalates to L2, never down) | LOW risk, code |
| L2 | bounded model review per partition | MEDIUM, LOW generated/lockfile |
| L3 | per-partition model review + cross-partition assessment | HIGH/PRIVILEGED, privileged paths |

`level_for(risk, file_class)` never returns below the risk minimum:
PRIVILEGED → L3, HIGH → ≥L2, MEDIUM → L2. The overall plan is the max over
files (`resolve_review_plan`). The existing `effective_risk`/`runtime_risk`
escalation in `review/handler.py` is unchanged.

`should_skip_rereview` returns true only for an unchanged candidate (same
base/result/diff hash) under the same policy revision — otherwise a changed
hash invalidates the verdict and a fresh review is required.

## 2. Partition manifest `review-partition-manifest.v1`

Contract: `docs/schemas/review-partition-manifest.v1.schema.json`.
Builder: `src/vuzol/review/partitions.py::build_manifest`.

- Files sorted, first-fit packing into partitions bounded by 80 files and
  120 000 chars per partition (the old whole-diff limits now apply per
  slice, so a >120k diff flows through partitions instead of BLOCKED
  "split the change").
- Invariants enforced fail-closed by `validate_manifest`: coverage (union ==
  changed files), no overlap, deterministic order and ids (`p00`, `p01`, …).
- Header normalization: `split_diff_by_file` parses both plain
  (`a/X b/X`) and git-quoted (`"a/..." "b/..."`, `core.quotePath=true`)
  headers, decoding octal escapes to the same `utf-8/surrogateescape` form
  as the `--name-only -z` file list; an unparseable header raises.
- Content coverage: every listed file must own a non-empty slice of the
  actual diff — a file with no delivered content raises
  `IndependentReviewError` (BLOCKED) instead of an empty slice passing
  silently.
- Inventory: `generated_inventory` + `lockfile_inventory` list every
  generated/lockfile path; per-partition `generated_files`/`lockfile_files`.
- Honesty: `diff_truncated` per partition and `truncated` overall replace the
  old hardcoded `diff_truncated: False`; a single-file partition that still
  exceeds the budget stays BLOCKED with an honest flag.
- Total cap: at most 8 partitions + 1 cross-partition call per review step;
  excess → `IndependentReviewError` → BLOCKED, never silent.

Chunk batches (model bundles) are hash-pinned and verified by
`verify_chunk_receipts`: duplicate chunk receipt or incomplete batches raise
instead of reviewing a corrupted bundle. The reviewer also verifies the batch
it builds before reserving budget.

## 3. Bounded review + aggregation `review-aggregate.v1`

Contract: `docs/schemas/review-aggregate.v1.schema.json`.
`IndependentModelReviewer.review` (`src/vuzol/review/independent.py`):

1. One bounded model call per partition through the existing accounting port
   (`DatabaseReviewAccounting`, `purpose="review"`; worker/repair allowance
   untouched). A blocker partition verdict stops the loop — remaining
   partitions are skipped without spending budget — and blocks the result.
2. One bounded cross-partition assessment over partition summaries (cross-file
   defects: contradictory changes, duplicated logic, smuggled injection).
3. Deterministic `aggregate_partition_verdicts`: any partition BLOCKED or any
   blocker finding → BLOCKED; error → changes_required; warning →
   pass_with_warnings. Empty verdict set or hash drift raises — aggregation
   failure is never PASS.

The retained diff is sent as **untrusted data** (`diff_untrusted: true`,
explicit non-following instruction); the cross-partition call receives only
summaries, never full diffs again.

Unknown price/usage is never zero: reconcile settles at the conservative
floor (`minimum_unknown_usage_cost`), the verdict carries
`unknown_usage: true` plus a summary note, and the cost export separates
known/unknown rows.

## 4. Hash binding

The verdict binds to base/result commit + `diff_hash`
(`review/handler.py`, `workflows/result_approval.py`). The handler re-inspects
the worktree after independent review: mutation mid-review → BLOCKED
(`review_failed`). Approval rejects `diff_hash` mismatch independently of
`result_commit` mismatch (separate test).

## 5. Cost export `review-cost-export.v1`

`review_cost_export(session, task_id=...)` reads the shared ledger
(`usage_totals_by_purpose` projection, `purpose="review"`) and returns
`invocations`, `total_cost_units`, split into `known_*` (`cost_known=true`)
and `unknown_*` (conservative floor, `unknown_is_floor_not_zero: true`).
WP02-binding-compatible: the manifest/aggregate payloads persist through the
existing `ArtifactStore` + `InputBinding(slot, schema, hash-pinned)` path.

## 6. Boundaries

No approval-semantics, permission, secret-scope or budget-semantics changes;
no Jev dependency; no installer rewrite; no remote nodes; no migration
(`result_validation.py` untouched — gate semantics unchanged). Heavy runs one
at a time; no merge, no push.
