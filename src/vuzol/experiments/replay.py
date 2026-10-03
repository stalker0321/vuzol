"""Deterministic replay of one fixed decision request (J5).

Given a recorded opportunity and the exact provider response it produced, the
replay explains each layer separately — parsing (strict validator), mapping
(label -> route hint) and application (advisory or a caller-supplied effect) —
so a route outcome can be attributed to the right layer.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum

from vuzol.experiments.decision_corpus import DecisionOpportunity
from vuzol.experiments.domain import FrozenModel
from vuzol.experiments.intake_shadow import (
    IntakeDecision,
    IntakeDecisionInvalid,
    IntakeEffect,
    parse_intake_output,
    route_hint,
)
from vuzol.interpretation.prompt_loader import PromptKind, prompt_hash


class ReplayStage(StrEnum):
    PARSING = "parsing"
    MAPPING = "mapping"
    APPLICATION = "application"


class StageResult(FrozenModel):
    stage: ReplayStage
    ok: bool
    detail: str


class ReplayTrace(FrozenModel):
    opportunity_id: str
    stages: tuple[StageResult, ...]
    route_hint: str | None = None
    effect: str | None = None
    target_ref: str | None = None
    applied: bool = False
    reason_code: str | None = None


def replay_intake(
    opportunity: DecisionOpportunity,
    raw_response: dict[str, object],
    *,
    application: Callable[[IntakeDecision], None] | None = None,
) -> ReplayTrace:
    """Explain parsing -> mapping -> application for a fixed request."""

    allowed_effects = (
        tuple(IntakeEffect(value) for value in opportunity.allowed_effects)
        if opportunity.allowed_effects
        else tuple(IntakeEffect)
    )
    prompt_digest = prompt_hash(PromptKind.INTAKE)
    stages: list[StageResult] = []
    try:
        decision = parse_intake_output(
            raw_response,
            allowed_effects=allowed_effects,
            allowed_refs=frozenset(opportunity.allowed_refs),
            prompt_digest=prompt_digest,
        )
    except IntakeDecisionInvalid as error:
        stages.append(StageResult(stage=ReplayStage.PARSING, ok=False, detail=str(error)))
        stages.append(
            StageResult(
                stage=ReplayStage.MAPPING,
                ok=False,
                detail="skipped: parsing failed",
            )
        )
        stages.append(
            StageResult(
                stage=ReplayStage.APPLICATION,
                ok=False,
                detail="skipped: no validated decision",
            )
        )
        return ReplayTrace(
            opportunity_id=opportunity.opportunity_id,
            stages=tuple(stages),
            reason_code="invalid_output",
        )

    stages.append(
        StageResult(
            stage=ReplayStage.PARSING,
            ok=True,
            detail=f"decision.v3 valid: effect={decision.effect.value} abstain={decision.abstain}",
        )
    )
    hint = route_hint(decision)
    stages.append(
        StageResult(
            stage=ReplayStage.MAPPING,
            ok=True,
            detail=f"route_hint={hint!r}",
        )
    )
    if decision.abstain:
        stages.append(
            StageResult(
                stage=ReplayStage.APPLICATION,
                ok=True,
                detail=f"abstain ({decision.reason_code.value}): no transition",
            )
        )
        return ReplayTrace(
            opportunity_id=opportunity.opportunity_id,
            stages=tuple(stages),
            route_hint=hint,
            effect=decision.effect.value,
            target_ref=decision.target_id,
            applied=False,
            reason_code=decision.reason_code.value,
        )
    if application is None:
        stages.append(
            StageResult(
                stage=ReplayStage.APPLICATION,
                ok=True,
                detail="advisory_only: caller owns the transition",
            )
        )
        return ReplayTrace(
            opportunity_id=opportunity.opportunity_id,
            stages=tuple(stages),
            route_hint=hint,
            effect=decision.effect.value,
            target_ref=decision.target_id,
            applied=False,
            reason_code="advisory_only",
        )
    application(decision)
    stages.append(
        StageResult(stage=ReplayStage.APPLICATION, ok=True, detail="effect applied by caller")
    )
    return ReplayTrace(
        opportunity_id=opportunity.opportunity_id,
        stages=tuple(stages),
        route_hint=hint,
        effect=decision.effect.value,
        target_ref=decision.target_id,
        applied=True,
        reason_code="applied",
    )
