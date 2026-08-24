"""Fail when the MVP readiness policy drifts from the reviewed registry.

The production readiness check pins provider-profile facts as Python dict
literals. When the registry changes without updating ``deploy/mvp/check.py``
(or vice versa) the mismatch only surfaced on the host during deployment.
This suite parses the check's literal expectations and requires every one of
them to still match exactly one enabled registry profile.
"""

import ast
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CHECK_PATH = ROOT / "deploy/mvp/check.py"
REGISTRY_PATH = ROOT / "deploy/registries.executor.toml"


def _profile_policy_dicts() -> list[dict[str, object]]:
    source = CHECK_PATH.read_text()
    function = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "_require_provider_profiles"
    )
    return [ast.literal_eval(node) for node in ast.walk(function) if isinstance(node, ast.Dict)]


def _matches(profile: dict[str, object], expected: dict[str, object]) -> bool:
    for key, value in expected.items():
        actual = profile.get(key)
        if isinstance(value, dict):
            if not isinstance(actual, dict) or not _matches(actual, value):
                return False
        elif actual != value:
            return False
    return True


def test_every_mvp_profile_expectation_matches_exactly_one_registry_profile() -> None:
    registry = tomllib.loads(REGISTRY_PATH.read_text())
    profiles = [profile for profile in registry["profiles"] if isinstance(profile, dict)]
    policies = [
        policy
        for policy in _profile_policy_dicts()
        if policy and any(key in policy for key in ("provider", "model", "roles"))
    ]

    assert policies, "no profile policy literals found in deploy/mvp/check.py"
    matched_profiles: set[str] = set()
    for policy in policies:
        matches = [
            str(profile["id"])
            for profile in profiles
            if _matches(profile, policy) and profile.get("id") not in matched_profiles
        ]
        assert len(matches) == 1, (
            f"policy {policy} does not match exactly one unused registry profile "
            f"(matched: {matches}); deploy/mvp/check.py drifted from the registry"
        )
        matched_profiles.add(matches[0])
