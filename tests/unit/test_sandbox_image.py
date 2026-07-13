"""Static invariants for the Codex sandbox image."""

from pathlib import Path

DOCKERFILE = Path(__file__).parents[2] / "Dockerfile.sandbox"


def test_sandbox_image_installs_tls_ca_bundle() -> None:
    content = DOCKERFILE.read_text()
    first_instruction = next(line for line in content.splitlines() if line.startswith("FROM "))
    assert first_instruction.startswith("FROM node:22-bookworm-slim@sha256:")
    assert len(first_instruction.rsplit("@sha256:", 1)[1]) == 64
    assert "apt-get install --yes --no-install-recommends ca-certificates" in content
    assert "rm -rf /var/lib/apt/lists/*" in content
