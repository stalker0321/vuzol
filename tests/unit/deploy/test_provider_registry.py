"""Static checks for the reviewed production provider registry."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_production_sandbox_uses_minimal_tooling_image() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())

    assert registry["sandboxes"][0]["id"] == "project-default"
    assert registry["sandboxes"][0]["image"] == (
        "vuzol-sandbox@sha256:8a32088414ff60dc7e94740811f55c100af7775640666df0ff59587d863a8b02"
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
