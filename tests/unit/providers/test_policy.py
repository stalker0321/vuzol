"""Policy tests (split for cohesion)."""

from __future__ import annotations

from ._test_providers_helpers import (
    Decimal,
    EffectiveProfileState,
    ExclusionReason,
    LaunchMode,
    NormalizedUsage,
    Path,
    ProviderRole,
    QuotaState,
    account_usage,
    estimate_reservation,
    profile,
    public_select_profile,
    routing_request,
    select_profile,
)


def test_policy_filters_and_orders_deterministically() -> None:
    cheap = profile("cheap", cost_class="cheap", routing_priority=20)
    preferred = profile("preferred", cost_class="balanced", routing_priority=10)
    saturated = profile("saturated", routing_priority=1)
    unhealthy = profile("unhealthy", routing_priority=0)
    states = {
        "cheap": EffectiveProfileState(),
        "preferred": EffectiveProfileState(),
        "saturated": EffectiveProfileState(active_leases=1),
        "unhealthy": EffectiveProfileState(healthy=False),
    }

    decision = select_profile(routing_request(), (unhealthy, saturated, cheap, preferred), states)

    assert decision.selected_profile_id == "preferred"
    assert decision.alternatives == ("cheap",)
    reasons = {item.profile_id: item.reasons for item in decision.evaluations}
    assert ExclusionReason.CONCURRENCY in reasons["saturated"]
    assert ExclusionReason.UNHEALTHY in reasons["unhealthy"]
    assert decision == select_profile(
        routing_request(), (preferred, cheap, saturated, unhealthy), states
    )


def test_code_execution_requires_cli_sandbox_profile() -> None:
    api = profile("api", sandbox_required=True)
    cli = profile(
        "cli",
        provider="codex",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="cli",
        state_directory=Path("/var/lib/codex-cli"),
        sandbox_required=True,
    )
    decision = select_profile(
        routing_request(requires_sandbox=True, required_launch_mode=LaunchMode.CLI),
        (api, cli),
        {"api": EffectiveProfileState(), "cli": EffectiveProfileState()},
    )
    assert decision.selected_profile_id == "cli"
    api_evaluation = next(item for item in decision.evaluations if item.profile_id == "api")
    assert ExclusionReason.LAUNCH_MODE in api_evaluation.reasons


def test_policy_honors_only_eligible_explicit_profile_and_fallback() -> None:
    primary = profile("primary", fallback_profile_ids=("fallback",))
    fallback = profile("fallback", routing_priority=999)
    forbidden = profile("forbidden", capabilities=frozenset())
    states = {item.id: EffectiveProfileState() for item in (primary, fallback, forbidden)}

    explicit = select_profile(
        routing_request(trusted_profile_id="forbidden"),
        (primary, fallback, forbidden),
        states,
    )
    assert explicit.selected_profile_id == "primary"
    fallback_decision = select_profile(
        routing_request(
            failed_profile_id="primary",
            allowed_fallback_ids=("fallback",),
        ),
        (primary, fallback, forbidden),
        states,
    )
    assert fallback_decision.selected_profile_id == "fallback"
    assert all(
        item.profile_id != "primary" or not item.eligible for item in fallback_decision.evaluations
    )


def test_policy_treats_quota_and_unknown_cost_conservatively() -> None:
    configured = profile("profile", minimum_unknown_usage_cost=0.5)
    quota = select_profile(
        routing_request(),
        (configured,),
        {"profile": EffectiveProfileState(quota_state=QuotaState.EXHAUSTED)},
    )
    assert quota.selected_profile_id is None
    budget = select_profile(
        routing_request(remaining_cost_units=0.1),
        (configured,),
        {"profile": EffectiveProfileState()},
    )
    assert budget.selected_profile_id is None
    estimate = estimate_reservation(configured, input_tokens=1, output_tokens=1)
    assert estimate.cost_units == Decimal("0.500000")


def test_policy_reports_every_static_security_exclusion() -> None:
    incompatible = profile(
        "incompatible",
        roles=frozenset({ProviderRole.PLANNER}),
        supported_task_types=frozenset({"coding"}),
        capabilities=frozenset(),
        sandbox_required=False,
        context_limit=10,
        output_limit=10,
    )
    request = routing_request(
        project_allowed_capabilities=frozenset(),
        estimated_input_tokens=100,
        max_output_tokens=100,
        requires_sandbox=True,
    )
    decision = public_select_profile(
        request, (incompatible,), {"incompatible": EffectiveProfileState()}
    )
    reasons = set(decision.evaluations[0].reasons)
    assert {
        ExclusionReason.ROLE,
        ExclusionReason.TASK_TYPE,
        ExclusionReason.CAPABILITY,
        ExclusionReason.PROJECT_POLICY,
        ExclusionReason.SANDBOX,
        ExclusionReason.CONTEXT_LIMIT,
        ExclusionReason.OUTPUT_LIMIT,
    }.issubset(reasons)


def test_usage_accounting_uses_configured_rates_and_quota() -> None:
    configured = profile(
        "priced",
        input_cost_units_per_million=2,
        output_cost_units_per_million=4,
        quota_units_per_call=3,
    )
    usage = account_usage(
        configured,
        NormalizedUsage(input_tokens=1_000, output_tokens=500, duration_ms=10),
    )
    assert usage.cost_units == Decimal("0.004000")
    assert usage.quota_units == Decimal("3.000000")

    supplied = account_usage(
        configured,
        NormalizedUsage(
            input_tokens=1,
            output_tokens=1,
            cost_units=Decimal("9"),
            quota_units=Decimal("8"),
            duration_ms=1,
        ),
    )
    assert supplied.cost_units == Decimal("9")
    assert supplied.quota_units == Decimal("8")
