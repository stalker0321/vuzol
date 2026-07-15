import json
from pathlib import Path

from pytest import MonkeyPatch

from vuzol.config import RegistryDocument, Settings, TopicConfig, TopicKind
from vuzol.config import runtime as runtime_module


def test_runtime_configuration_loads_registry_file_once(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    registry_file = tmp_path / "registries.toml"
    registry_file.write_text("")
    configured = Settings(
        environment="test",
        registry_file=registry_file,
        repository_root=tmp_path / "repositories",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    runtime_module.get_runtime_configuration.cache_clear()
    monkeypatch.setattr(runtime_module, "get_settings", lambda: configured)

    first = runtime_module.get_runtime_configuration()
    second = runtime_module.get_runtime_configuration()

    assert first is second
    assert first.settings is configured
    assert first.registries.projects.items() == ()

    runtime_module.get_runtime_configuration.cache_clear()


def test_runtime_configuration_allows_no_registry_file(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    configured = Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    runtime_module.get_runtime_configuration.cache_clear()
    monkeypatch.setattr(runtime_module, "get_settings", lambda: configured)

    assert runtime_module.get_runtime_configuration().registries.profiles.items() == ()

    runtime_module.get_runtime_configuration.cache_clear()


def test_runtime_configuration_merges_dynamic_registry_overlay(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    registry_file = tmp_path / "registries.toml"
    registry_file.write_text("")
    overlay_file = tmp_path / "projects.json"
    overlay_file.write_text(
        json.dumps(
            RegistryDocument(
                topics=(
                    TopicConfig(
                        chat_id=-100,
                        message_thread_id=3,
                        kind=TopicKind.INBOX,
                        default_workflow="project_provisioning",
                    ),
                )
            ).model_dump(mode="json")
        )
    )
    configured = Settings(
        environment="test",
        registry_file=registry_file,
        registry_overlay_file=overlay_file,
        repository_root=tmp_path / "repositories",
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    runtime_module.get_runtime_configuration.cache_clear()
    monkeypatch.setattr(runtime_module, "get_settings", lambda: configured)

    runtime = runtime_module.get_runtime_configuration()
    assert runtime.registries.topics.resolve(-100, 3).kind is TopicKind.INBOX

    runtime_module.get_runtime_configuration.cache_clear()
