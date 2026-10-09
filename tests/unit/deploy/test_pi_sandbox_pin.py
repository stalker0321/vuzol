"""The sandbox image must pin the Pi package and provide /pi-home (T083).

Account-bound pi profiles moved to the untracked local overlay (T085); the
tracked registry only keeps provider facts such as the opencode.ai egress.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_sandbox_dockerfile_pins_pi_package_and_home() -> None:
    dockerfile = (ROOT / "Dockerfile.sandbox").read_text(encoding="utf-8")
    assert "ARG PI_CODING_AGENT_VERSION=1.0.3" in dockerfile
    assert '"@earendil-works/pi-coding-agent@${PI_CODING_AGENT_VERSION}"' in dockerfile
    assert "/pi-home" in dockerfile
    # Base image node version must satisfy the package engines (>=22.19.0).
    assert "node:22-bookworm-slim" in dockerfile


def test_executor_registry_excludes_account_bound_profiles() -> None:
    registry = (ROOT / "deploy/registries.executor.toml").read_text(encoding="utf-8")
    assert 'provider = "pi"' not in registry
    assert "state_directory" not in registry
    # The provider egress fact stays in the tracked project network policy.
    assert '{ url = "https://opencode.ai", purpose = "Pi opencode-go inference" }' in registry
