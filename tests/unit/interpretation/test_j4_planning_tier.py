"""J4 code-owned planning tier: selection, budget mapping, compiler wiring."""

from __future__ import annotations

import uuid

from tests.unit.providers._test_providers_helpers import (
    BudgetMode,
    EffectiveProfileState,
    ProviderRole,
    profile,
    routing_request,
    select_profile,
)
from vuzol.interpretation.domain import (
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.interpretation.planning import (
    PLANNING_POLICY_VERSION,
    PlanningTier,
    budget_mode_for_tier,
    resolve_planning_tier,
    tier_needs_planning,
)
from vuzol.storage.types import RiskLevel
from vuzol.workflows.compiler import compile_workflow


def _draft(
    *,
    complexity: SuggestedComplexity = SuggestedComplexity.SMALL,
    risk: RiskLevel = RiskLevel.LOW,
    outcomes: tuple[str, ...] = (),
    missing: tuple[str, ...] = (),
    needs_planning: bool = False,
) -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        goal="Implement the request",
        task_summary="Implement the requested change",
        requested_outcomes=outcomes,
        missing_information=missing,
        suggested_complexity=complexity,
        suggested_risk=risk,
        needs_planning=needs_planning,
        needs_clarification=False,
        normalized_title="Implement request",
    )


def test_resolve_planning_tier_rules() -> None:
    assert resolve_planning_tier(_draft()).tier is PlanningTier.DIRECT
    # Small diff does not skip needed planning.
    assert resolve_planning_tier(_draft(), uncertainty=True).tier is PlanningTier.LIGHT
    assert resolve_planning_tier(_draft(), dependency_count=2).tier is PlanningTier.LIGHT
    # Large mechanical change is not escalated by size alone.
    large = _draft(complexity=SuggestedComplexity.LARGE)
    assert resolve_planning_tier(large).tier is PlanningTier.LIGHT
    # Large with dependencies/uncertainty, or high risk, is STRONG.
    assert resolve_planning_tier(large, dependency_count=2).tier is PlanningTier.STRONG
    assert resolve_planning_tier(_draft(risk=RiskLevel.HIGH)).tier is PlanningTier.STRONG


def test_required_gap_requires_scout_not_strong_without_evidence() -> None:
    gap = _draft(risk=RiskLevel.HIGH, missing=("which database?",))
    decision = resolve_planning_tier(gap, required_gaps=gap.missing_information)
    assert decision.requires_scout is True
    assert decision.tier is not PlanningTier.STRONG
    assert "required_gap_requires_scout" in decision.reasons

    with_evidence = resolve_planning_tier(
        _draft(risk=RiskLevel.HIGH, missing=("which database?",)),
        required_gaps=("which database?",),
        evidence=True,
    )
    assert with_evidence.requires_scout is False
    assert with_evidence.tier is PlanningTier.STRONG


def test_budget_mapping_and_policy_version() -> None:
    assert PLANNING_POLICY_VERSION == "planning-policy.v1"
    assert budget_mode_for_tier(PlanningTier.DIRECT) == "cheap"
    assert budget_mode_for_tier(PlanningTier.LIGHT) == "balanced"
    assert budget_mode_for_tier(PlanningTier.STRONG) == "strong"
    assert tier_needs_planning(PlanningTier.DIRECT) is False
    assert tier_needs_planning(PlanningTier.LIGHT) is True
    assert tier_needs_planning(PlanningTier.STRONG) is True


def test_compiler_uses_code_owned_tier_over_draft_boolean() -> None:
    interpretation_id = uuid.uuid4()
    direct = compile_workflow(
        _draft(needs_planning=True),
        interpretation_id=interpretation_id,
        planning_tier=PlanningTier.DIRECT,
    )
    assert "plan" not in [step.key for step in direct.steps]

    light = compile_workflow(
        _draft(needs_planning=False),
        interpretation_id=interpretation_id,
        planning_tier=PlanningTier.LIGHT,
    )
    strong = compile_workflow(
        _draft(needs_planning=False),
        interpretation_id=interpretation_id,
        planning_tier=PlanningTier.STRONG,
    )
    assert "plan" in [step.key for step in light.steps]
    assert "plan" in [step.key for step in strong.steps]

    # No tier supplied: legacy in-memory boolean still decides.
    fallback = compile_workflow(_draft(needs_planning=True), interpretation_id=interpretation_id)
    assert "plan" in [step.key for step in fallback.steps]


def test_plan_step_evidence_reaches_worker_context() -> None:
    from types import SimpleNamespace

    from vuzol.providers.planner_handoff import load_planner_context_for_run
    from vuzol.storage.types import StepStatus

    plan_step = SimpleNamespace(
        id=uuid.uuid4(),
        step_type="plan",
        status=StepStatus.COMPLETED,
        result={"text": "1. Read the code\n2. Apply the change"},
    )
    items = load_planner_context_for_run(plan_step)  # type: ignore[arg-type]
    assert items
    assert items[0].source == "workflow_plan_result"
    # DIRECT has no plan step, so the worker gets no planning context.
    assert load_planner_context_for_run(None) == ()


def test_light_and_strong_dispatch_different_planner_cost_class() -> None:
    balanced = profile(
        "planner-balanced",
        cost_class="balanced",
        roles=frozenset({ProviderRole.PLANNER}),
        supported_task_types=frozenset({"coding"}),
    )
    strong = profile(
        "planner-strong",
        cost_class="strong",
        roles=frozenset({ProviderRole.PLANNER}),
        supported_task_types=frozenset({"coding"}),
    )
    states = {
        "planner-balanced": EffectiveProfileState(),
        "planner-strong": EffectiveProfileState(),
    }

    light_request = routing_request(
        role=ProviderRole.PLANNER,
        task_type="coding",
        budget_mode=BudgetMode.BALANCED,
    )
    strong_request = routing_request(
        role=ProviderRole.PLANNER,
        task_type="coding",
        budget_mode=BudgetMode.STRONG,
    )
    assert select_profile(light_request, (balanced, strong), states).selected_profile_id == (
        "planner-balanced"
    )
    assert select_profile(strong_request, (balanced, strong), states).selected_profile_id == (
        "planner-strong"
    )
