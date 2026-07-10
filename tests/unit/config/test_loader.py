from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config import (
    Capability,
    ConfigurationLoadError,
    ProjectConfig,
    ProviderProfileConfig,
    RegistryDocument,
    Settings,
    TopicConfig,
    TopicKind,
    build_bundle,
    load_document,
)


def settings(tmp_path: Path, **changes: object) -> Settings:
    values: dict[str, object] = {
        "environment": "test",
        "repository_root": tmp_path / "repositories",
        "artifact_root": tmp_path / "artifacts",
        "secret_file_root": tmp_path / "secrets",
    }
    values.update(changes)
    return Settings.model_validate(values)


def profile(**changes: object) -> ProviderProfileConfig:
    values: dict[str, object] = {
        "id": "profile-a",
        "provider": "provider",
        "model": "model",
        "launch_mode": "api",
        "credential_reference": "env:PROFILE_KEY",
        "capabilities": frozenset({Capability.REPOSITORY_READ}),
        "concurrency_limit": 1,
        "cost_class": "balanced",
        "supported_task_types": frozenset({"general"}),
    }
    values.update(changes)
    return ProviderProfileConfig.model_validate(values)


def test_example_toml_loads_into_strict_document() -> None:
    document = load_document(Path("config/registries.example.toml"))

    assert document.projects[0].id == "example"
    assert document.profiles[0].id == "example-api"
    assert document.topics[0].kind == "inbox"


def test_invalid_toml_and_schema_fail_with_file_context(tmp_path: Path) -> None:
    malformed = tmp_path / "malformed.toml"
    malformed.write_text("[[projects]\n")
    with pytest.raises(ConfigurationLoadError, match="invalid registry file"):
        load_document(malformed)

    invalid_schema = tmp_path / "invalid-schema.toml"
    invalid_schema.write_text("[[projects]]\nid = 'UPPERCASE'\n")
    with pytest.raises(ConfigurationLoadError, match="invalid registry file"):
        load_document(invalid_schema)


def test_bundle_validates_required_profile_and_system_secrets(tmp_path: Path) -> None:
    configured = settings(
        tmp_path,
        database_dsn_reference="env:DATABASE_DSN",
        telegram_bot_token_reference="env:TELEGRAM_TOKEN",  # noqa: S106
    )
    document = RegistryDocument(profiles=(profile(),))

    with pytest.raises(ConfigurationLoadError, match="PROFILE_KEY"):
        build_bundle(document, configured, environment={})

    with pytest.raises(ConfigurationLoadError, match="DATABASE_DSN"):
        build_bundle(document, configured, environment={"PROFILE_KEY": "profile-secret"})

    bundle = build_bundle(
        document,
        configured,
        environment={
            "PROFILE_KEY": "profile-secret",
            "DATABASE_DSN": "database-secret",
            "TELEGRAM_TOKEN": "telegram-secret",
        },
    )
    assert bundle.profiles.get("profile-a").id == "profile-a"


def test_bundle_revision_is_stable_and_contains_no_secret_value(tmp_path: Path) -> None:
    document = RegistryDocument(profiles=(profile(),))
    configured = settings(tmp_path)
    first = build_bundle(document, configured, environment={"PROFILE_KEY": "secret-one"})
    second = build_bundle(document, configured, environment={"PROFILE_KEY": "secret-two"})

    assert first.revision == second.revision
    assert "secret-one" not in repr(first)
    assert "secret-two" not in repr(second)


def test_bundle_rejects_unknown_topic_project(tmp_path: Path) -> None:
    topic = TopicConfig(
        chat_id=-1,
        message_thread_id=1,
        kind=TopicKind.PROJECT,
        project_id="missing",
        default_workflow="coding_task",
    )
    with pytest.raises(ConfigurationLoadError, match="unknown project ID"):
        build_bundle(RegistryDocument(topics=(topic,)), settings(tmp_path), environment={})


def test_settings_require_absolute_roots_and_positive_nested_limits(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="must be absolute"):
        Settings(repository_root=Path("relative"))
    with pytest.raises(ValidationError, match="greater than 0"):
        Settings.model_validate(
            {
                "repository_root": tmp_path,
                "artifact_root": tmp_path,
                "secret_file_root": tmp_path,
                "limits": {"task_cost_units": 0},
            }
        )
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        Settings.model_validate(
            {
                "repository_root": tmp_path,
                "artifact_root": tmp_path,
                "secret_file_root": tmp_path,
                "concurrency": {"heavy": 0},
            }
        )
    with pytest.raises(ValidationError, match="less than or equal to 50"):
        Settings.model_validate(
            {
                "repository_root": tmp_path,
                "artifact_root": tmp_path,
                "secret_file_root": tmp_path,
                "database": {"pool_size": 51},
            }
        )


def test_enabled_project_is_normalized_in_bundle(tmp_path: Path) -> None:
    repository = tmp_path / "repositories" / "project-a"
    repository.mkdir(parents=True)
    configured_project = ProjectConfig(
        id="project-a",
        display_name="Project A",
        repository_path=Path("project-a"),
        default_branch="main",
        allowed_capabilities=frozenset({Capability.REPOSITORY_READ}),
        sandbox_profile="project-default",
    )
    bundle = build_bundle(
        RegistryDocument(projects=(configured_project,)), settings(tmp_path), environment={}
    )
    assert bundle.projects.get("project-a").repository_path == repository.resolve()
