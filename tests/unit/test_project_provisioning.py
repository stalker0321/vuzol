import asyncio
import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest import MonkeyPatch

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
    load_document,
    merge_documents,
)
from vuzol.projects.provisioning import (
    FixedSystemdReloader,
    ProjectProvisioningService,
    RegistryOverlayWriter,
    run_provisioning_loop,
)
from vuzol.storage.models import ProjectProvisioning


def test_registry_overlay_adds_one_inherited_project_and_topic_idempotently(
    tmp_path: Path,
) -> None:
    repository_root = tmp_path / "repositories"
    repository_root.mkdir()
    (repository_root / "vuzol").mkdir()
    (repository_root / "notes").mkdir()
    base_path = tmp_path / "base.json"
    overlay_path = tmp_path / "projects.json"
    settings = Settings(
        environment="test",
        repository_root=repository_root,
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        registry_file=base_path,
        registry_overlay_file=overlay_path,
    )
    base = RegistryDocument(
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
                message_thread_id=3,
                kind=TopicKind.INBOX,
                display_name="Новый проект",
                default_workflow="simple_model_task",
            ),
        ),
        sandboxes=(
            SandboxProfileConfig(
                id="project-default",
                image=f"example/sandbox@sha256:{'0' * 64}",
            ),
        ),
    )
    base_path.write_text(json.dumps(base.model_dump(mode="json")))
    runtime = RuntimeConfiguration(settings=settings, registries=build_bundle(base, settings))
    provisioning = ProjectProvisioning(
        task_id=uuid.uuid4(),
        requested_by_user_id=42,
        chat_id=-100,
        source_thread_id=3,
        project_id="notes",
        display_name="Notes",
        description="A note-taking app",
        repository_path="notes",
        topic_thread_id=41,
    )

    writer = RegistryOverlayWriter(runtime, overlay_path)
    first_revision = writer.add_project(provisioning)
    second_revision = writer.add_project(provisioning)
    assert first_revision == second_revision
    overlay = load_document(overlay_path)
    assert [project.id for project in overlay.projects] == ["notes"]
    assert overlay.projects[0].git_delivery.allowed_modes
    assert overlay.topics[0].message_thread_id == 41
    assert overlay.topics[0].default_workflow == "adaptive_task"
    merged = build_bundle(merge_documents(base, overlay), settings)
    assert merged.projects.get("notes").repository_path == repository_root / "notes"
    assert merged.topics.resolve(-100, 41).project_id == "notes"

    collision = ProjectProvisioning(
        task_id=uuid.uuid4(),
        requested_by_user_id=42,
        chat_id=-100,
        source_thread_id=3,
        project_id="other",
        display_name="Other",
        description="Another project",
        repository_path="other",
        topic_thread_id=41,
    )
    with pytest.raises(ValueError, match="topic is already assigned"):
        writer.add_project(collision)

    no_base_settings = settings.model_copy(update={"registry_file": None})
    no_base_runtime = RuntimeConfiguration(
        settings=no_base_settings,
        registries=runtime.registries,
    )
    with pytest.raises(ValueError, match="static registry file is required"):
        RegistryOverlayWriter(no_base_runtime, overlay_path).add_project(provisioning)

    fresh_overlay = tmp_path / "fresh-projects.json"
    collision.topic_thread_id = 42
    with pytest.raises(ValueError, match="static registry file is required"):
        RegistryOverlayWriter(no_base_runtime, fresh_overlay).add_project(collision)


@pytest.mark.anyio
async def test_fixed_systemd_reloader_uses_only_bounded_units(
    monkeypatch: MonkeyPatch,
) -> None:
    process = AsyncMock()
    process.returncode = 0
    process.communicate.return_value = (b"", b"")
    create = AsyncMock(return_value=process)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)

    await FixedSystemdReloader().reload()

    create.assert_awaited_once_with(
        "systemctl",
        "try-restart",
        "vuzol-executor.service",
        "vuzol-telegram.service",
        "vuzol-telegram-delivery.service",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )


@pytest.mark.anyio
async def test_fixed_systemd_reloader_reports_failure(monkeypatch: MonkeyPatch) -> None:
    process = AsyncMock()
    process.returncode = 1
    process.communicate.return_value = (b"", b"permission denied")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))

    with pytest.raises(OSError, match="permission denied"):
        await FixedSystemdReloader().reload()


def test_project_provisioner_requires_dynamic_registry() -> None:
    settings = Settings(environment="test")
    runtime = RuntimeConfiguration(
        settings=settings,
        registries=build_bundle(RegistryDocument(), settings),
    )

    with pytest.raises(ValueError, match="registry_overlay_file is required"):
        ProjectProvisioningService(
            runtime,
            MagicMock(),
            MagicMock(),
            owner="test",
            reloader=MagicMock(),
        )


@pytest.mark.anyio
async def test_provisioning_loop_polls_until_stopped() -> None:
    stop_event = asyncio.Event()
    service = AsyncMock()

    async def process_one() -> bool:
        stop_event.set()
        return False

    service.process_one.side_effect = process_one

    await run_provisioning_loop(
        service,
        poll_interval_seconds=0.001,
        stop_event=stop_event,
    )

    service.process_one.assert_awaited_once()


@pytest.mark.anyio
async def test_provisioning_loop_rechecks_immediately_after_work() -> None:
    stop_event = asyncio.Event()
    service = AsyncMock()

    async def process_one() -> bool:
        stop_event.set()
        return True

    service.process_one.side_effect = process_one

    await run_provisioning_loop(
        service,
        poll_interval_seconds=1,
        stop_event=stop_event,
    )

    service.process_one.assert_awaited_once()
