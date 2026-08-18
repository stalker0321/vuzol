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
    reconcile_imported_environments,
    run_provisioning_loop,
)
from vuzol.storage.models import ProjectProvisioning


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


@pytest.mark.anyio
async def test_reconcile_imported_environments_records_only_existing_repositories(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    repository_root = tmp_path / "repositories"
    (repository_root / "present").mkdir(parents=True)
    settings = Settings(environment="test", repository_root=repository_root)
    runtime = RuntimeConfiguration(
        settings=settings,
        registries=build_bundle(RegistryDocument(), settings),
    )
    rows = [
        MagicMock(project_id="present", repository_path="present"),
        MagicMock(project_id="missing", repository_path="missing"),
    ]
    session = MagicMock()
    session.scalars = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=rows)))
    factory = MagicMock()
    factory.begin.return_value = AsyncContext(session)
    record = AsyncMock()
    monkeypatch.setattr(
        "vuzol.projects.provisioning.record_detected_environment",
        record,
    )

    count = await reconcile_imported_environments(runtime, factory)

    assert count == 1
    record.assert_awaited_once_with(
        session,
        project_id="present",
        repository=repository_root / "present",
    )


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        ({"Makefile": "test:\n\ttrue\n"}, ("make", "test")),
        ({"package.json": '{"scripts":{"test":"node --test"}}'}, ("npm", "test")),
        ({"pyproject.toml": "[project]\nname='demo'\n"}, ("pytest",)),
        ({"pytest.ini": "[pytest]\n"}, ("pytest",)),
        ({"server.js": "console.log('ok')\n"}, ("node", "--test")),
        ({"README.md": "# Demo\n"}, None),
    ],
)
def test_import_validation_detection_is_conservative(
    tmp_path: Path,
    files: dict[str, str],
    expected: tuple[str, ...] | None,
) -> None:
    for name, content in files.items():
        (tmp_path / name).write_text(content)

    commands = RegistryOverlayWriter._import_validation_commands(tmp_path)

    if expected is None:
        assert commands == ()
    else:
        assert len(commands) == 1
        assert commands[0].argv == expected


@pytest.mark.parametrize("content", ["not json", "{}", '{"scripts":{}}'])
def test_import_package_without_test_script_does_not_invent_command(
    tmp_path: Path, content: str
) -> None:
    (tmp_path / "package.json").write_text(content)

    assert RegistryOverlayWriter._import_validation_commands(tmp_path) == ()


@pytest.mark.parametrize(
    "server_marker",
    ["server.js", "app.py", "manage.py", "Dockerfile", "compose.yaml", "docker-compose.yml"],
)
def test_import_server_marker_disables_static_delivery(tmp_path: Path, server_marker: str) -> None:
    (tmp_path / "index.html").write_text("<main>Demo</main>")
    (tmp_path / server_marker).write_text("")

    assert RegistryOverlayWriter._import_static_deployment(tmp_path, "demo") is None


def test_import_static_site_gets_project_scoped_delivery(tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text("<main>Demo</main>")

    deployment = RegistryOverlayWriter._import_static_deployment(tmp_path, "demo")

    assert deployment is not None
    assert deployment.url_path == "demo"


def test_import_non_web_project_has_no_delivery(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("# Library")

    assert RegistryOverlayWriter._import_static_deployment(tmp_path, "library") is None


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
    assert overlay.topics[0].pinned is True
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
        "vuzol-worker.service",
        "vuzol-applier.service",
        "vuzol-static-publisher-worker.service",
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
