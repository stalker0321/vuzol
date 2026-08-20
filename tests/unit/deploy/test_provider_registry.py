"""Static checks for the reviewed production provider registry."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_production_sandbox_uses_minimal_tooling_image() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())

    assert registry["sandboxes"][0]["id"] == "project-default"
    assert registry["sandboxes"][0]["image"] == (
        "vuzol-sandbox@sha256:cc7ce7ecc67abc52000a53bc2efe1d3bf975d8f7ce1282fb37f37ade53125897"
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


def test_production_kimi_profile_is_pinned_to_free_model() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    profiles = [profile for profile in registry["profiles"] if profile.get("provider") == "kimi"]

    assert len(profiles) == 1
    assert profiles[0]["id"] == "tokenrouter-kimi-a"
    assert profiles[0]["model"] == "moonshotai/kimi-k3-free"


def test_production_planner_uses_deepseek_via_deepinfra_with_router_fallbacks() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    profile = next(
        profile
        for profile in registry["profiles"]
        if profile["id"] == "openrouter-deepseek-planner-prod"
    )

    assert profile["model"] == "deepseek/deepseek-v4-flash-0731"
    assert profile["api_base_url"] == "https://openrouter.ai/api/v1"
    assert profile["credential_reference"] == "env:VUZOL_OPENROUTER_PLANNER_API_KEY"
    assert profile["roles"] == ["planner", "reviewer"]
    assert profile["provider_routing"] == {
        "order": ["deepinfra/fp8"],
        "allow_fallbacks": True,
    }


def test_nvidia_glm_worker_profile_is_prepared_but_not_routable_without_agent_transport() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    profile = next(profile for profile in registry["profiles"] if profile["id"] == "nvidia-glm-5-2")

    assert profile["model"] == "z-ai/glm-5.2"
    assert profile["api_base_url"] == "https://integrate.api.nvidia.com/v1"
    assert profile["credential_reference"] == "env:VUZOL_NVIDIA_API_KEY"
    assert profile["roles"] == ["executor"]
    assert profile["enabled"] is False


def test_production_cli_workers_share_one_provider_neutral_contract() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    profiles = {
        profile["id"]: profile
        for profile in registry["profiles"]
        if profile.get("provider") in {"codex", "grok", "kimi"}
    }

    assert set(profiles) == {
        "codex-subscription-prod",
        "grok-subscription-a",
        "grok-subscription-b",
        "tokenrouter-kimi-a",
    }
    for profile in profiles.values():
        assert profile["launch_mode"] == "cli"
        assert profile["sandbox_required"] is True
        assert "executor" in profile["roles"]
        assert set(profile["capabilities"]) >= {
            "repository_read",
            "code_edit",
            "git",
            "project_shell",
        }

    for profile_id in {
        "codex-subscription-prod",
        "grok-subscription-a",
        "grok-subscription-b",
        "tokenrouter-kimi-a",
    }:
        contract = profiles[profile_id]["agent_runtime_contract"]
        assert contract["working_directory"] == "/workspace"
        assert contract["writable_roots"] == ["/workspace"]
        assert contract["protected_roots"] == ["/workspace/.git"]
        assert contract["supports_read"] is True
        assert contract["supports_search"] is True
        assert contract["supports_edit"] is True
        assert contract["supports_git"] is False
        assert contract["supports_network"] is False
