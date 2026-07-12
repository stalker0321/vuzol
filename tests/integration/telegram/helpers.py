from pathlib import Path

from vuzol.config import (
    Capability,
    ProjectConfig,
    RegistryDocument,
    RuntimeConfiguration,
    SandboxProfileConfig,
    Settings,
    TopicConfig,
    TopicKind,
    build_bundle,
)


def telegram_runtime(tmp_path: Path) -> RuntimeConfiguration:
    repository_root = tmp_path / "repositories"
    project_path = repository_root / "vuzol"
    project_path.mkdir(parents=True)
    settings = Settings(
        environment="test",
        allowed_user_ids=(42,),
        allowed_chat_ids=(-100,),
        repository_root=repository_root,
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
    )
    document = RegistryDocument(
        projects=(
            ProjectConfig(
                id="vuzol",
                display_name="Vuzol",
                repository_path=Path("vuzol"),
                default_branch="main",
                allowed_capabilities=frozenset({Capability.REPOSITORY_READ}),
                sandbox_profile="project-default",
            ),
        ),
        topics=(
            TopicConfig(
                chat_id=-100,
                message_thread_id=10,
                kind=TopicKind.PROJECT,
                project_id="vuzol",
                default_workflow="coding_task",
            ),
        ),
        sandboxes=(
            SandboxProfileConfig(
                id="project-default",
                image=f"example/sandbox@sha256:{'0' * 64}",
            ),
        ),
    )
    return RuntimeConfiguration(settings=settings, registries=build_bundle(document, settings))
