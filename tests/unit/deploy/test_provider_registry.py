"""Static checks for the reviewed production provider registry."""

import tomllib
from pathlib import Path

from vuzol.config.models import BudgetMode, ProviderProfileConfig, ProviderRole
from vuzol.providers.domain import EffectiveProfileState
from vuzol.providers.policy import RoutingRequest, select_profile

ROOT = Path(__file__).resolve().parents[3]


def test_production_sandbox_uses_minimal_tooling_image() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())

    assert registry["sandboxes"][0]["id"] == "project-default"
    assert registry["sandboxes"][0]["image"] == (
        "vuzol-sandbox@sha256:f9febc02fac6547ade58dab77c4806fcc5de1d772907139de310453abee8ee3c"
    )


def test_production_grok_profiles_use_current_model_id() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    grok_profiles = {
        profile["id"]: profile
        for profile in registry["profiles"]
        if profile.get("provider") == "grok"
    }

    assert set(grok_profiles) == {"grok-subscription-a", "grok-subscription-b"}
    assert {profile["model"] for profile in grok_profiles.values()} == {"grok-4.5"}


def test_production_kimi_profile_is_bounded_to_model_only_heavy_roles() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    profiles = {profile["id"]: profile for profile in registry["profiles"]}

    kimi = profiles["kimi-k3-free"]
    assert kimi == {
        "id": "kimi-k3-free",
        "provider": "openai-compatible",
        "model": "moonshotai/kimi-k3-free",
        "api_base_url": "https://api.tokenrouter.com/v1",
        "launch_mode": "api",
        "credential_reference": "env:TOKENROUTER_KIMI_API_KEY",
        "credential_required": True,
        "capabilities": [],
        "concurrency_limit": 2,
        "context_limit": 1_000_000,
        "output_limit": 65_536,
        "cost_class": "strong",
        "roles": ["planner"],
        "routing_priority": 40,
        "supported_task_types": [
            "architecture",
            "coding",
            "research",
            "infrastructure",
            "file_processing",
            "general",
        ],
        "fallback_profile_ids": ["openai-planner-prod"],
        "sandbox_required": False,
        "minimum_unknown_usage_cost": 0.001,
        "enabled": True,
    }


def test_production_kimi_routes_only_strong_planning() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    profiles = tuple(
        ProviderProfileConfig.model_validate(profile)
        for profile in registry["profiles"]
        if profile["id"] in {"kimi-k3-free", "openai-planner-prod"}
    )

    def selected(mode: BudgetMode) -> str | None:
        return select_profile(
            RoutingRequest(
                role=ProviderRole.PLANNER,
                task_type="coding",
                required_capabilities=frozenset(),
                project_allowed_capabilities=frozenset(),
                budget_mode=mode,
                estimated_input_tokens=1_000,
                max_output_tokens=1_000,
                remaining_cost_units=1.0,
                required_launch_mode=profiles[0].launch_mode,
            ),
            profiles,
            {profile.id: EffectiveProfileState() for profile in profiles},
        ).selected_profile_id

    assert selected(BudgetMode.BALANCED) == "openai-planner-prod"
    assert selected(BudgetMode.STRONG) == "kimi-k3-free"
