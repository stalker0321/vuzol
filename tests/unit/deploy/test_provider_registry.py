"""Static checks for the reviewed production provider registry.

Account-bound CLI profiles (codex/grok/kimi/pi accounts) deliberately live in
the untracked local overlay now, so this file only pins the provider facts that
stay in the tracked registry. Overlay contents are checked by the local
``verify-account-profiles.py`` script outside the repository, not here.
"""

import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


def _registry() -> dict[str, Any]:
    return tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())


def test_production_sandbox_uses_minimal_tooling_image() -> None:
    registry = _registry()

    assert registry["sandboxes"][0]["id"] == "project-default"
    assert registry["sandboxes"][0]["image"] == (
        "vuzol-sandbox@sha256:cc7ce7ecc67abc52000a53bc2efe1d3bf975d8f7ce1282fb37f37ade53125897"
    )


def test_tracked_registry_carries_no_account_bound_profiles() -> None:
    profiles = _registry()["profiles"]

    account_bound = [profile["id"] for profile in profiles if "state_directory" in profile]
    assert account_bound == []


def test_production_planner_uses_deepseek_via_deepinfra_with_router_fallbacks() -> None:
    profile = next(
        profile
        for profile in _registry()["profiles"]
        if profile["id"] == "openrouter-deepseek-planner-prod"
    )

    assert profile["model"] == "deepseek/deepseek-v4-flash-0731"
    assert profile["api_base_url"] == "https://openrouter.ai/api/v1"
    assert profile["credential_reference"] == "env:VUZOL_OPENROUTER_PLANNER_API_KEY"
    assert profile["roles"] == ["planner"]
    assert profile["output_limit"] == 8_000
    assert profile["provider_routing"] == {
        "sort": {"by": "price", "partition": "none"},
        "preferred_min_throughput": {"p90": 70},
        "quantizations": ["int8", "fp8"],
        "allow_fallbacks": True,
    }


def test_production_reviewer_uses_mimo_via_openrouter_with_low_effort() -> None:
    profile = next(
        profile
        for profile in _registry()["profiles"]
        if profile["id"] == "openrouter-mimo-reviewer-prod"
    )

    assert profile["model"] == "xiaomi/mimo-v2.5"
    assert profile["model_reasoning_effort"] == "low"
    assert profile["api_base_url"] == "https://openrouter.ai/api/v1"
    assert profile["credential_reference"] == "env:VUZOL_OPENROUTER_REVIEWER_API_KEY"
    assert profile["credential_required"] is True
    assert profile["launch_mode"] == "api"
    assert profile["roles"] == ["reviewer"]
    assert profile["output_limit"] == 8_000
    assert profile["sandbox_required"] is False
    assert profile["enabled"] is True


def test_nvidia_glm_worker_profile_is_prepared_but_not_routable_without_agent_transport() -> None:
    profile = next(
        profile for profile in _registry()["profiles"] if profile["id"] == "nvidia-glm-5-2"
    )

    assert profile["model"] == "z-ai/glm-5.2"
    assert profile["api_base_url"] == "https://integrate.api.nvidia.com/v1"
    assert profile["credential_reference"] == "env:VUZOL_NVIDIA_API_KEY"
    assert profile["roles"] == ["executor"]
    assert profile["enabled"] is False
