# ADR-0021 — Code-owned planning tier wired into compiler and dispatcher

Status: accepted (J4, base d4f4c25). Implements `IMPLEMENTATION_PLAN.md` §J4.
Builds on ADR-0015 (D4 semantic planning) and ADR-0019 (context assembler).

## 1. Versioned planning policy

`interpretation/planning.py` gains `PLANNING_POLICY_VERSION`
(``planning-policy.v1``) and a code-owned `resolve_planning_tier` that wraps the
pure `select_planning_tier` with two rules from the plan:

- A deterministic required gap (`draft.missing_information`) is routed to
  Scout/user input: without evidence it is never "solved" by a STRONG planning
  call (`requires_scout=True`, tier at most LIGHT).
- A small diff does not skip needed planning (uncertainty/dependencies lift it
  to at least LIGHT), and a large mechanical change is not escalated to STRONG
  by size alone.

`PlanningDecision.event_payload` carries the tier, policy version, spec
revision, requires_scout and reasons.

## 2. Persisted, spec-bound tier

The tier is computed in the interpretation service (never supplied by the model)
and written as an `Event` (`task.planning_tier_selected`) bound to the task's
`spec_revision`. The write is idempotent: a re-tick of the same spec revision
reuses the existing event (binding-check) instead of writing a second verdict.
No migration and no new column: the existing Event ledger is the persisted
metadata.

## 3. Compiler reads the tier

`compile_workflow` accepts `planning_tier`. When supplied it is authoritative
for the optional `plan` step (`tier_needs_planning`); when absent the in-memory
`draft.needs_planning` boolean remains the fallback, so existing callers and
tests are unchanged. DIRECT therefore materializes no plan step; LIGHT/STRONG
do.

## 4. Dispatcher reads the tier and sets the budget mode

`WorkflowDispatcher._planning_tier` reads the event matching the current spec
revision, passes it to `compile_workflow`, and sets the run's `budget_mode`
(DIRECT→cheap, LIGHT→balanced, STRONG→strong). `budget_mode` orders provider
cost classes in `providers/policy._COST_ORDER`, so LIGHT and STRONG dispatch a
different planner cost class while the role (PLANNER) stays the same. Shortage
or budget never silently downgrades: the tier is not derived from availability.

## 5. Planning evidence reaches the worker

Because the plan step is now actually materialized, the existing planner
handoff (`load_planner_context_for_run`) attaches validated plan text to the
executor request as context items. DIRECT has no plan step and therefore no
planning context.

## 6. Floors unchanged

The tier is a spend hint only. Review, acceptance and approval floors, and
capabilities/permissions, stay exactly as they are on every tier
(`review/policy.py`, `interpretation/policy.py` untouched).

## Consequences

- J5 can compare DIRECT/LIGHT/STRONG routes by workflow, budget mode and full
  provider call count.
- The tier is code-owned; a future rubric/mapping change must bump
  `PLANNING_POLICY_VERSION` so persisted verdicts stay traceable.
