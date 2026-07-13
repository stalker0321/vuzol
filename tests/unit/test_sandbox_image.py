"""Static invariants for the Codex sandbox image."""

from pathlib import Path

DOCKERFILE = Path(__file__).parents[2] / "Dockerfile.sandbox"


def test_sandbox_image_installs_tls_ca_bundle() -> None:
    content = DOCKERFILE.read_text()
    first_instruction = next(line for line in content.splitlines() if line.startswith("FROM "))
    assert first_instruction.startswith("FROM node:22-bookworm-slim@sha256:")
    assert len(first_instruction.rsplit("@sha256:", 1)[1]) == 64
    assert "apt-get install --yes --no-install-recommends ca-certificates curl" in content
    assert "rm -rf /var/lib/apt/lists/*" in content


def test_sandbox_image_pins_and_verifies_grok_binary() -> None:
    content = DOCKERFILE.read_text()
    assert "ARG GROK_VERSION=0.2.99" in content
    assert (
        "ARG GROK_SHA256=9fccba400d3808ec34a991892096b34c6f5846b2b118d355001601fd5428445c"
        in content
    )
    assert "sha256sum --check --strict" in content
