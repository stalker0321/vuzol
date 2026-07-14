"""Static invariants for the Codex sandbox image."""

from pathlib import Path

DOCKERFILE = Path(__file__).parents[2] / "Dockerfile.sandbox"
VALIDATION_DOCKERFILE = Path(__file__).parents[2] / "Dockerfile.validation"


def test_sandbox_image_installs_tls_ca_bundle() -> None:
    content = DOCKERFILE.read_text()
    first_instruction = next(line for line in content.splitlines() if line.startswith("FROM "))
    assert first_instruction.startswith("FROM node:22-bookworm-slim@sha256:")
    assert len(first_instruction.rsplit("@sha256:", 1)[1]) == 64
    assert "apt-get install --yes --no-install-recommends ca-certificates curl git" in content
    assert "rm -rf /var/lib/apt/lists/*" in content


def test_sandbox_image_contains_git_for_isolated_worker_commits() -> None:
    content = DOCKERFILE.read_text()
    assert "--no-install-recommends ca-certificates curl git" in content


def test_sandbox_image_pins_and_verifies_grok_binary() -> None:
    content = DOCKERFILE.read_text()
    assert "ARG GROK_VERSION=0.2.99" in content
    assert (
        "ARG GROK_SHA256=9fccba400d3808ec34a991892096b34c6f5846b2b118d355001601fd5428445c"
        in content
    )
    assert "sha256sum --check --strict" in content


def test_validation_image_is_lock_driven_and_contains_no_project_source() -> None:
    content = VALIDATION_DOCKERFILE.read_text()
    from_lines = [line for line in content.splitlines() if line.startswith("FROM ")]
    assert all("@sha256:" in line for line in from_lines)
    assert "ghcr.io/astral-sh/uv:0.11.28@sha256:" in content
    assert "python:3.12.11-slim-bookworm@sha256:" in content
    assert "apt-get install --yes --no-install-recommends acl git make postgresql-15" in content
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
    assert "/opt/vuzol-postgres-template" in content
