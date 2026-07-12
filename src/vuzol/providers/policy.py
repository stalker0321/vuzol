"""Pure deterministic provider eligibility and ordering policy."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from vuzol.config.models import (
    BudgetMode,
    Capability,
    CostClass,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.providers.domain import EffectiveProfileState, QuotaState


class ExclusionReason(StrEnum):
    DISABLED = "disabled"
    ROLE = "role"
    TASK_TYPE = "task_type"
    CAPABILITY = "capability"
    PROJECT_POLICY = "project_policy"
    SANDBOX = "sandbox"
    UNHEALTHY = "unhealthy"
    RATE_LIMITED = "rate_limited"
    QUOTA = "quota"
    CONTEXT_LIMIT = "context_limit"
    OUTPUT_LIMIT = "output_limit"
    BUDGET = "budget"
    CONCURRENCY = "concurrency"
    NOT_CONFIGURED_FALLBACK = "not_configured_fallback"


@dataclass(frozen=True, slots=True)
class RoutingRequest:
    role: ProviderRole
    task_type: str
    required_capabilities: frozenset[Capability]
    project_allowed_capabilities: frozenset[Capability] | None
    budget_mode: BudgetMode
    estimated_input_tokens: int
    max_output_tokens: int
    remaining_cost_units: float
    trusted_profile_id: str | None = None
    failed_profile_id: str | None = None
    allowed_fallback_ids: tuple[str, ...] = ()
    requires_sandbox: bool = False


@dataclass(frozen=True, slots=True)
class ProfileEvaluation:
    profile_id: str
    eligible: bool
    reasons: tuple[ExclusionReason, ...]


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    selected_profile_id: str | None
    alternatives: tuple[str, ...]
    evaluations: tuple[ProfileEvaluation, ...]


_COST_ORDER: dict[BudgetMode, dict[CostClass, int]] = {
    BudgetMode.CHEAP: {CostClass.CHEAP: 0, CostClass.BALANCED: 1, CostClass.STRONG: 2},
    BudgetMode.BALANCED: {CostClass.BALANCED: 0, CostClass.CHEAP: 1, CostClass.STRONG: 2},
    BudgetMode.STRONG: {CostClass.STRONG: 0, CostClass.BALANCED: 1, CostClass.CHEAP: 2},
}


def select_profile(
    request: RoutingRequest,
    profiles: tuple[ProviderProfileConfig, ...],
    states: Mapping[str, EffectiveProfileState],
    *,
    now: datetime | None = None,
) -> PolicyDecision:
    """Return a stable decision without mutating state or resolving credentials."""

    observed_at = now or datetime.now(UTC)
    evaluations: list[ProfileEvaluation] = []
    eligible: list[ProviderProfileConfig] = []
    for profile in sorted(profiles, key=lambda item: item.id):
        state = states.get(profile.id, EffectiveProfileState())
        reasons = _exclusions(request, profile, state, observed_at)
        evaluations.append(
            ProfileEvaluation(profile_id=profile.id, eligible=not reasons, reasons=tuple(reasons))
        )
        if not reasons:
            eligible.append(profile)

    def ordering(profile: ProviderProfileConfig) -> tuple[int, int, int, int, int, int, str]:
        state = states.get(profile.id, EffectiveProfileState())
        explicit_rank = 0 if request.trusted_profile_id == profile.id else 1
        fallback_rank = (
            request.allowed_fallback_ids.index(profile.id)
            if request.failed_profile_id is not None and profile.id in request.allowed_fallback_ids
            else len(request.allowed_fallback_ids)
        )
        return (
            explicit_rank,
            fallback_rank,
            _COST_ORDER[request.budget_mode][profile.cost_class],
            profile.routing_priority,
            state.active_leases,
            state.queue_depth,
            profile.id,
        )

    ordered = tuple(profile.id for profile in sorted(eligible, key=ordering))
    return PolicyDecision(
        selected_profile_id=ordered[0] if ordered else None,
        alternatives=ordered[1:],
        evaluations=tuple(evaluations),
    )


def _exclusions(
    request: RoutingRequest,
    profile: ProviderProfileConfig,
    state: EffectiveProfileState,
    now: datetime,
) -> list[ExclusionReason]:
    reasons: list[ExclusionReason] = []
    if not profile.enabled:
        reasons.append(ExclusionReason.DISABLED)
    if request.role not in profile.roles:
        reasons.append(ExclusionReason.ROLE)
    if request.task_type not in profile.supported_task_types:
        reasons.append(ExclusionReason.TASK_TYPE)
    if not request.required_capabilities.issubset(profile.capabilities):
        reasons.append(ExclusionReason.CAPABILITY)
    if (
        request.project_allowed_capabilities is not None
        and not request.required_capabilities.issubset(request.project_allowed_capabilities)
    ):
        reasons.append(ExclusionReason.PROJECT_POLICY)
    if request.requires_sandbox and not profile.sandbox_required:
        reasons.append(ExclusionReason.SANDBOX)
    if not state.healthy and (state.unhealthy_until is None or state.unhealthy_until > now):
        reasons.append(ExclusionReason.UNHEALTHY)
    if state.rate_limit_until is not None and state.rate_limit_until > now:
        reasons.append(ExclusionReason.RATE_LIMITED)
    if state.quota_state is QuotaState.EXHAUSTED:
        reasons.append(ExclusionReason.QUOTA)
    if profile.context_limit is not None and request.estimated_input_tokens > profile.context_limit:
        reasons.append(ExclusionReason.CONTEXT_LIMIT)
    if profile.output_limit is not None and request.max_output_tokens > profile.output_limit:
        reasons.append(ExclusionReason.OUTPUT_LIMIT)
    if request.remaining_cost_units < profile.minimum_unknown_usage_cost:
        reasons.append(ExclusionReason.BUDGET)
    if state.active_leases >= profile.concurrency_limit:
        reasons.append(ExclusionReason.CONCURRENCY)
    if (
        request.failed_profile_id is not None
        and profile.id != request.trusted_profile_id
        and profile.id not in request.allowed_fallback_ids
    ):
        reasons.append(ExclusionReason.NOT_CONFIGURED_FALLBACK)
    return reasons
