"""Unit-level accounting rules: cached tokens are never priced twice."""

from __future__ import annotations

from decimal import Decimal

from vuzol.config import ProviderProfileConfig
from vuzol.providers.budgets import account_usage
from vuzol.providers.domain import NormalizedUsage


def _priced_profile() -> ProviderProfileConfig:
    return ProviderProfileConfig.model_validate(
        {
            "id": "api",
            "provider": "openai-compatible",
            "model": "model",
            "api_base_url": "https://provider.example/v1",
            "launch_mode": "api",
            "credential_required": False,
            "capabilities": frozenset(),
            "concurrency_limit": 1,
            "cost_class": "balanced",
            "roles": frozenset({"executor"}),
            "supported_task_types": frozenset({"general"}),
            "sandbox_required": False,
            "input_cost_units_per_million": 2.0,
            "output_cost_units_per_million": 4.0,
            "minimum_unknown_usage_cost": 0.01,
        }
    )


def test_cached_tokens_do_not_change_the_priced_cost() -> None:
    profile = _priced_profile()
    base = account_usage(
        profile, NormalizedUsage(input_tokens=1_000, output_tokens=1_000, duration_ms=1)
    )
    cached = account_usage(
        profile,
        NormalizedUsage(
            input_tokens=1_000, output_tokens=1_000, cached_tokens=800, duration_ms=1
        ),
    )
    assert base.cost_units == Decimal("0.006")
    assert cached.cost_units == base.cost_units
    assert cached.cached_tokens == 800


def test_missing_rates_leave_cost_unknown_not_zero() -> None:
    profile = ProviderProfileConfig.model_validate(
        {
            "id": "api",
            "provider": "openai-compatible",
            "model": "model",
            "api_base_url": "https://provider.example/v1",
            "launch_mode": "api",
            "credential_required": False,
            "capabilities": frozenset(),
            "concurrency_limit": 1,
            "cost_class": "balanced",
            "roles": frozenset({"executor"}),
            "supported_task_types": frozenset({"general"}),
            "sandbox_required": False,
            "minimum_unknown_usage_cost": 0.01,
        }
    )
    accounted = account_usage(
        profile, NormalizedUsage(input_tokens=1_000, output_tokens=1_000, duration_ms=1)
    )
    assert accounted.cost_units is None
