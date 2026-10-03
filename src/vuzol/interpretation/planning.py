"""Planning spend tiers (D4 W6): DIRECT / LIGHT / STRONG.

Policy classification of planning cost only. A tier never changes scope,
permissions, capabilities, approvals, or the review floor — those stay in
``interpretation/policy.py`` and ``review/policy.py``.

Selection inputs (ARCHITECTURE_REVIEW §5.4): uncertainty, cross-subsystem
dependencies, effect risk, expected result size / acceptance units, and the
cost of a wrong strategy. A long mechanical job may be DIRECT; a short
irreversible change may require STRONG.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from vuzol.interpretation.domain import SuggestedComplexity, TaskDraft
from vuzol.storage.types import RiskLevel

# J4: the planning policy is versioned so a persisted tier can be traced to the
# rubric/mapping that produced it. Bump on any selection change.
PLANNING_POLICY_VERSION = "planning-policy.v1"
PLANNING_TIER_EVENT = "task.planning_tier_selected"

_RISK_ORDER = {
    RiskLevel.LOW: 0,
    RiskLevel.MEDIUM: 1,
    RiskLevel.HIGH: 2,
    RiskLevel.PRIVILEGED: 3,
}


class PlanningTier(StrEnum):
    DIRECT = "direct"
    LIGHT = "light"
    STRONG = "strong"


# Tier -> persisted run budget mode. LIGHT and STRONG dispatch different
# provider cost classes (see providers/policy._COST_ORDER); DIRECT adds no plan
# step at all.
_TIER_BUDGET_MODE = {
    PlanningTier.DIRECT: "cheap",
    PlanningTier.LIGHT: "balanced",
    PlanningTier.STRONG: "strong",
}


@dataclass(frozen=True, slots=True)
class PlanningDecision:
    """Code-owned planning verdict bound to a spec revision."""

    tier: PlanningTier
    policy_version: str
    requires_scout: bool
    reasons: tuple[str, ...]

    def event_payload(self, *, spec_revision: str | None) -> dict[str, object]:
        return {
            "tier": self.tier.value,
            "policy_version": self.policy_version,
            "spec_revision": spec_revision,
            "requires_scout": self.requires_scout,
            "reasons": list(self.reasons),
        }


def select_planning_tier(
    draft: TaskDraft,
    *,
    dependency_count: int = 0,
    uncertainty: bool = False,
) -> PlanningTier:
    """Pure policy hint for planning spend. No authority attached."""

    risk_high = _RISK_ORDER[draft.suggested_risk] >= 2
    large = draft.suggested_complexity is SuggestedComplexity.LARGE
    many_deps = dependency_count >= 2
    if risk_high or (large and (many_deps or uncertainty)):
        return PlanningTier.STRONG
    if large or many_deps or uncertainty or len(draft.requested_outcomes) >= 4:
        return PlanningTier.LIGHT
    return PlanningTier.DIRECT


def tier_needs_planning(tier: PlanningTier) -> bool:
    """Map a tier to the workflow ``needs_planning`` flag value."""

    return tier is not PlanningTier.DIRECT


def budget_mode_for_tier(tier: PlanningTier) -> str:
    """Map a tier to the persisted run budget mode (role/profile ordering)."""

    return _TIER_BUDGET_MODE[tier]


def resolve_planning_tier(
    draft: TaskDraft,
    *,
    required_gaps: tuple[str, ...] = (),
    dependency_count: int = 0,
    uncertainty: bool = False,
    evidence: bool = False,
) -> PlanningDecision:
    """Versioned code-owned planning verdict.

    Rules (IMPLEMENTATION_PLAN §J4 / ARCHITECTURE_REVIEW §5.4):

    - A deterministic required gap is routed to Scout/user input; without
      evidence it is never "solved" by a STRONG planning call.
    - A small diff does not skip needed planning: uncertainty or cross-subsystem
      dependencies lift it to at least LIGHT.
    - A large mechanical change is not escalated to STRONG by size alone.
    """

    reasons: list[str] = []
    large = draft.suggested_complexity is SuggestedComplexity.LARGE
    many_deps = dependency_count >= 2
    if required_gaps and not evidence:
        tier = PlanningTier.LIGHT if (large or many_deps or uncertainty) else PlanningTier.DIRECT
        return PlanningDecision(
            tier=tier,
            policy_version=PLANNING_POLICY_VERSION,
            requires_scout=True,
            reasons=("required_gap_requires_scout",),
        )
    if required_gaps:
        reasons.append("required_gap_with_evidence")
    tier = select_planning_tier(draft, dependency_count=dependency_count, uncertainty=uncertainty)
    if tier is PlanningTier.STRONG:
        reasons.append("strong_policy_match")
    elif tier is PlanningTier.LIGHT:
        reasons.append("light_policy_match")
    else:
        reasons.append("direct_policy_match")
    return PlanningDecision(
        tier=tier,
        policy_version=PLANNING_POLICY_VERSION,
        requires_scout=False,
        reasons=tuple(reasons),
    )
