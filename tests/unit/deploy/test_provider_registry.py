"""Static checks for the reviewed production provider registry."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_production_grok_profiles_use_current_model_id() -> None:
    registry = tomllib.loads((ROOT / "deploy/registries.executor.toml").read_text())
    grok_profiles = {
        profile["id"]: profile
        for profile in registry["profiles"]
        if profile.get("provider") == "grok"
    }

    assert set(grok_profiles) == {"grok-subscription-a", "grok-subscription-b"}
    assert {profile["model"] for profile in grok_profiles.values()} == {"grok-4.5"}
