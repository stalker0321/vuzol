from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config import (
    Capability,
    ConfigurationLoadError,
    ProviderProfileConfig,
    RegistryDocument,
    Settings,
    build_bundle,
)
from vuzol.config.settings import ChatSettings


def settings(tmp_path: Path, **changes: object) -> Settings:
    return Settings.model_validate(
        {
            "environment": "test",
            "repository_root": tmp_path / "repositories",
            "artifact_root": tmp_path / "artifacts",
            "secret_file_root": tmp_path / "secrets",
            **changes,
        }
    )


def test_chat_is_opt_in_and_requires_explicit_responder() -> None:
    assert not ChatSettings().enabled
    with pytest.raises(ValidationError, match=r"explicit chat\.profile_id"):
        ChatSettings(enabled=True)


@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {
            "launch_mode": "api",
            "api_base_url": "https://provider.example/v1",
            "runtime_identity": None,
            "state_directory": None,
        },
        {"sandbox_required": False},
        {"roles": frozenset({"planner"})},
        {"capabilities": frozenset()},
    ],
)
def test_chat_rejects_unsafe_or_incompatible_responder(
    tmp_path: Path, changes: dict[str, object]
) -> None:
    responder = chat_profile(tmp_path, **changes)
    configured = settings(tmp_path, chat=ChatSettings(enabled=True, profile_id=responder.id))
    with pytest.raises(ConfigurationLoadError, match="sandboxed CLI responder"):
        build_bundle(RegistryDocument(profiles=(responder,)), configured)


def chat_profile(tmp_path: Path, **changes: object) -> ProviderProfileConfig:
    return ProviderProfileConfig.model_validate(
        {
            "id": "sol-chat",
            "model": "gpt-6.1-sol",
            "provider": "codex",
            "concurrency_limit": 1,
            "cost_class": "balanced",
            "runtime_identity": "chat-test",
            "state_directory": tmp_path / "codex-state",
            "launch_mode": "cli",
            "credential_required": False,
            "credential_reference": None,
            "sandbox_required": True,
            "roles": frozenset({"executor"}),
            "capabilities": frozenset({Capability.REPOSITORY_READ}),
            "supported_task_types": frozenset({"architecture"}),
            **changes,
        }
    )


@pytest.mark.parametrize(
    "task_types",
    [frozenset({"coding"}), frozenset({"project_lead"}), frozenset({"general"}), frozenset()],
)
def test_chat_does_not_require_architecture_execution_support(tmp_path, task_types):
    responder = chat_profile(tmp_path, supported_task_types=task_types)
    configured = settings(tmp_path, chat=ChatSettings(enabled=True, profile_id=responder.id))
    bundle = build_bundle(RegistryDocument(profiles=(responder,)), configured)
    assert bundle.profiles.get(responder.id).supported_task_types == task_types


def test_chat_profile_is_not_required_by_legacy_configuration(tmp_path: Path) -> None:
    build_bundle(RegistryDocument(), settings(tmp_path))


def test_chat_rejects_unknown_responder(tmp_path: Path) -> None:
    configured = settings(tmp_path, chat=ChatSettings(enabled=True, profile_id="missing"))
    with pytest.raises(ConfigurationLoadError):
        build_bundle(RegistryDocument(), configured)
