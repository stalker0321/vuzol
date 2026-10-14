"""Static invariants for the Codex sandbox image."""

from pathlib import Path

DOCKERFILE = Path(__file__).parents[2] / "Dockerfile.sandbox"
VALIDATION_DOCKERFILE = Path(__file__).parents[2] / "Dockerfile.validation"


def test_sandbox_image_installs_tls_ca_bundle() -> None:
    content = DOCKERFILE.read_text()
    first_instruction = next(line for line in content.splitlines() if line.startswith("FROM "))
    assert first_instruction.startswith("FROM node:22-bookworm-slim@sha256:")
    assert len(first_instruction.rsplit("@sha256:", 1)[1]) == 64
    assert "apt-get install --yes --no-install-recommends ca-certificates curl git make" in content
    assert "rm -rf /var/lib/apt/lists/*" in content


def test_sandbox_image_contains_minimal_grok_iteration_tools() -> None:
    content = DOCKERFILE.read_text()
    assert "--no-install-recommends ca-certificates curl git make" in content
    assert "python3" not in content


def test_sandbox_image_pins_and_verifies_grok_binary() -> None:
    content = DOCKERFILE.read_text()
    assert "ARG GROK_VERSION=1.0.41" in content
    assert (
        "ARG GROK_SHA256=9ce03ed23e16ea01072b4496263d6213a27899e1e3e107f008d36edf82e70407"
        in content
    )
    assert "sha256sum --check --strict" in content


def test_binary_only_grok_upgrade_preserves_base_and_identity() -> None:
    content = (DOCKERFILE.parent / "Dockerfile.sandbox-grok-upgrade").read_text()
    assert (
        "ARG BASE_IMAGE=vuzol-sandbox@sha256:"
        "e75a5358bf1f4a5a67de29bdb33de4721321161bbf58abeedda6ee9856150d15"
        in content
    )
    assert "FROM ${BASE_IMAGE}" in content
    assert "COPY --chmod=0755 grok-1.0.41-linux-x86_64 /usr/local/bin/grok" in content
    assert "9ce03ed23e16ea01072b4496263d6213a27899e1e3e107f008d36edf82e70407" in content
    assert 'grok 1.0.41 (4220f3b224a6)' in content
    assert content.rstrip().endswith("USER 10001:10001\nWORKDIR /workspace")
    assert "apt-get" not in content and "npm install" not in content


def test_sandbox_image_pins_safe_chain_and_enforces_package_age() -> None:
    content = DOCKERFILE.read_text()
    assert "ARG SAFE_CHAIN_VERSION=1.5.15" in content
    assert "SAFE_CHAIN_INSTALLER_SHA256=" in content
    assert "--ci --install-dir /opt/safe-chain" in content
    assert "SAFE_CHAIN_MINIMUM_PACKAGE_AGE_HOURS=48" in content
    assert "PATH=/opt/safe-chain/shims:/opt/safe-chain/bin:" in content


def test_validation_image_is_lock_driven_and_contains_no_project_source() -> None:
    content = VALIDATION_DOCKERFILE.read_text()
    from_lines = [line for line in content.splitlines() if line.startswith("FROM ")]
    assert all("@sha256:" in line for line in from_lines)
    assert "ghcr.io/astral-sh/uv:0.11.28@sha256:" in content
    assert "python:3.12.11-slim-bookworm@sha256:" in content
    assert (
        "apt-get install --yes --no-install-recommends "
        "acl ca-certificates curl git make postgresql-15"
    ) in content
    assert "COPY pyproject.toml uv.lock ./" in content
    assert "--no-install-project" in content
    assert "vuzol-offline-dependency-audit" in content
    assert "pip-audit.json" in content
    assert "sha256sum pyproject.toml uv.lock" in content
    assert "COPY src" not in content
    assert "COPY tests" not in content
    assert "UV_NO_SYNC=1" in content
    assert "UV_OFFLINE=1" in content
    assert "vuzol-offline-test" in content
    assert "ARG GITLEAKS_VERSION=8.28.0" in content
    assert "vuzol-secret-scan" in content
    assert "/opt/vuzol-postgres-template" in content
