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

from enum import StrEnum

from vuzol.interpretation.domain import SuggestedComplexity, TaskDraft
from vuzol.storage.types import RiskLevel

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
