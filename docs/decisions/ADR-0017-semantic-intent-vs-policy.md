# ADR-0017 — Semantic intent belongs to the producer, not to policy

Status: accepted (J0 baseline, base 47692cc). Implements
`IMPLEMENTATION_PLAN.md` §J0. Refines ADR-0003 and ADR-0015: the bounded
semantic producer decides what a turn means; deterministic policy may only
tighten or reject the validated draft, never widen intent from a substring.

## Context

`enforce_interpretation_policy` treated any `_IMPLEMENTATION_MARKERS` substring
in `original_input.casefold()` as implementation intent and rewrote the draft
to `CREATE_TASK/CODING` with `{REPOSITORY_READ, CODE_EDIT}`
(`explicit_implementation_reclassified_as_coding`). Because the match was a raw
substring, quoted and negated wording was amplified: the reviewed probes

- `Ничего не меняй. Объясни, почему команда «удали» опасна.`
- `Не выполняй «добавь поле», только объясни смысл просьбы.`

were turned from `general_conversation` into executable coding tasks
(`evidence/probe-results.json`). The same substitution also let a design
question containing "как лучше удалить…" override the design/discussion branch.
This makes current behavior unusable as a truth label for Jev (R12).

## Decision

1. **Producer owns meaning.** Arbitrary natural language with quotes, scope or
   negation is resolved by the semantic producer (model interpreter). Policy
   maps a validated draft to a closed decision; it does not infer a new effect
   from substrings.
2. **Directed markers only.** `_IMPLEMENTATION_MARKERS` count only when the
   occurrence is outside a quoted segment and is not governed by a negation cue
   in the same clause (`_has_directed_marker`). A marker inside `«…»`, `“…”`,
   `"…"` etc., or after "не / ничего / нельзя / никогда / not / never", does not
   arm implementation intent.
3. **Design/discussion precedence.** A directed design marker
   (`_DESIGN_DISCUSSION_MARKERS`) suppresses implementation amplification, so a
   "how best to …" question stays read-only (architecture) instead of becoming
   `code_edit`.
4. **Explicit commands stay deterministic.** `/task`, `задача:`, `task:` remain
   the only deterministic command grammar (`explicit.py`); `/task` continues to
   bypass the producer. Explicit imperatives keep their current guards and
   reclassification (`test_classification.py`).
5. **Two entry points, measured separately.** The Task interpreter path
   (`policy.enforce_interpretation_policy`, called from
   `service._process_interpretation`) and the discussion confirm-first path
   (`discussion.enforce_discussion_policy`) are distinct routes. Discussion
   already forces `should_create_task=False`; it gains a regression test so the
   two paths are never conflated.
6. **Baseline tests are split.** Classifier (`classify_decision`), mapping
   (policy) and the actual transition (integration) are asserted separately,
   including that the materialized workflow contains no `code_edit` /
   `filesystem_write` / `git` step and no `execute_code`/`prepare_worktree`.

## Consequences

- Quoted and negated read-only requests no longer reach coding execution
  eligibility; design questions are not silently converted to edits.
- A genuine imperative ("Удали X") is unchanged and still carries its guards.
- Policy can still tighten or clarify, but never widen the effect/relation
  triple: `policy_allowed` mirrors `automatic_execution_eligible` as before.
- The fix is defensive (fewer amplifications). Any residual NL ambiguity is a
  producer concern for later J-packages, not a reason to restore substring
  widening in policy.
