"""Static checks for the reviewed production provider registry."""

import tomllib
from pathlib import Path

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
