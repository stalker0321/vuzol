"""Registry tests (split for cohesion)."""

from __future__ import annotations

from ._test_providers_helpers import *


def test_adapter_registry_resolves_only_selected_api_profile(tmp_path: Path) -> None:
    configured = profile(
        "api",
        credential_reference="env:API_KEY",
    )
    resolver = ScopedSecretResolver(
        access_policy={"env:API_KEY": frozenset({"profile:api"})},
        secret_file_root=tmp_path,
        environment={"API_KEY": "scoped-value"},  # pragma: allowlist secret
    )
    registry = AdapterRegistry(ProfileRegistry((configured,)), resolver)
    assert isinstance(registry.get("api"), OpenAICompatibleAdapter)
    assert registry.get("api") is registry.get("api")

    cli = configured.model_copy(
        update={
            "id": "cli",
            "provider": "codex",
            "api_base_url": None,
            "launch_mode": LaunchMode.CLI,
            "credential_reference": None,
            "credential_required": False,
            "runtime_identity": "codex",
            "state_directory": tmp_path / "codex",
        }
    )
    with pytest.raises(ValueError, match="Step 08"):
        AdapterRegistry(ProfileRegistry((cli,)), resolver).get("cli")
