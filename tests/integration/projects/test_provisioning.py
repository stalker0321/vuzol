import asyncio
import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select, update

from vuzol.config import (
    Capability,
    ProjectConfig,
    RegistryDocument,
    RuntimeConfiguration,
    SandboxProfileConfig,
    Settings,
    build_bundle,
)
from vuzol.interpretation.domain import (
    ProjectNameOption,
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.projects.provisioning import ProjectProvisioningService
from vuzol.storage.models import (
    Interpretation,
    ProjectNamingRequest,
    ProjectProvisioning,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    DeliveryStatus,
    ProjectNamingStatus,
    ProjectProvisioningStatus,
    RiskLevel,
    TaskStatus,
)
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.telegram.workspace import TopicCreationOutcomeUnknown
from vuzol.workflows.dispatch import WorkflowDispatcher

from ..storage.helpers import storage

pytestmark = pytest.mark.postgresql


class FakeWorkspaceClient:
    def __init__(self, *, thread_id: int = 41, fail: bool = False) -> None:
        self.thread_id = thread_id
        self.fail = fail
        self.created: list[tuple[int, str]] = []

    async def rename_topic(self, *, chat_id: int, thread_id: int, name: str) -> None:
        return None

    async def create_topic(self, *, chat_id: int, name: str) -> int:
        self.created.append((chat_id, name))
        if self.fail:
            raise TopicCreationOutcomeUnknown("lost response")
        return self.thread_id


class FakeReloader:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.calls = 0

    async def reload(self) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("reload unavailable")


def project_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_PROJECT,
        task_type=TaskType.INFRASTRUCTURE,
        operation=TaskOperation.CREATE,
        project_name_options=tuple(
            ProjectNameOption(display_name=f"Notes {index + 1}", project_id=f"notes-{index + 1}")
            for index in range(9)
        ),
        goal="A private note-taking application",
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=RiskLevel.LOW,
        needs_planning=False,
        needs_clarification=False,
        normalized_title="Create Notes",
    )


def runtime(tmp_path: Path) -> RuntimeConfiguration:
    repository_root = tmp_path / "repositories"
    repository_root.mkdir()
    (repository_root / "vuzol").mkdir()
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
        sandboxes=(
            SandboxProfileConfig(
                id="project-default",
                image=f"example/sandbox@sha256:{'0' * 64}",
            ),
        ),
    )
    base_path.write_text(json.dumps(document.model_dump(mode="json")))
    return RuntimeConfiguration(settings=settings, registries=build_bundle(document, settings))


async def seed_provisioning(factory: object) -> tuple[uuid.UUID, uuid.UUID]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    async with typed.begin() as session:
        task = Task(
            user_id=42,
            source_chat_id=-100,
            source_thread_id=3,
            project_id="notes",
            original_text="Create Notes",
            task_type="infrastructure",
            status=TaskStatus.EXECUTING,
        )
        session.add(task)
        await session.flush()
        provisioning = ProjectProvisioning(
            task_id=task.id,
            requested_by_user_id=42,
            chat_id=-100,
            source_thread_id=3,
            project_id="notes",
            display_name="Notes",
            description="A private note-taking application",
            repository_path="notes",
            status=ProjectProvisioningStatus.PENDING,
        )
        session.add(provisioning)
        await session.flush()
        outbox = TransactionalOutbox(
            destination="project_provisioning",
            operation_type="create_project",
            linked_entity_type="project_provisioning",
            linked_entity_id=provisioning.id,
            idempotency_key=f"project:provision:{provisioning.id}",
            payload={"project_id": "notes"},
        )
        session.add(outbox)
        await session.flush()
        return provisioning.id, outbox.id


def test_dispatcher_materializes_one_project_naming_request(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = runtime(tmp_path)
        draft = project_draft()
        async with factory.begin() as session:
            task = Task(
                user_id=42,
                source_chat_id=-100,
                source_thread_id=3,
                original_text="Create Notes",
                task_type="infrastructure",
                task_draft=draft.model_dump(mode="json"),
                status=TaskStatus.INTERPRETED,
            )
            session.add(task)
            await session.flush()
            interpretation = Interpretation(
                task_id=task.id,
                original_input_hash="a" * 64,
                task_draft=draft.model_dump(mode="json"),
                profile_id="fake",
                model="fake",
                prompt_version="project-intake-v1",
                schema_version="1.1",
            )
            session.add(interpretation)
            await session.flush()
            session.add(
                TransactionalOutbox(
                    destination="workflow_dispatch",
                    operation_type="dispatch_interpretation",
                    linked_entity_type="interpretation",
                    linked_entity_id=interpretation.id,
                    idempotency_key=f"workflow:dispatch:{interpretation.id}",
                    payload={"task_id": str(task.id)},
                )
            )
            task_id = task.id
        dispatcher = WorkflowDispatcher(configured, factory, owner="dispatcher")
        assert await dispatcher.process_one()
        async with factory() as session:
            persisted_task = await session.get(Task, task_id)
            naming = await session.scalar(
                select(ProjectNamingRequest).where(ProjectNamingRequest.task_id == task_id)
            )
            queued = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.linked_entity_type == "project_naming"
                )
            )
            assert persisted_task is not None
            assert persisted_task.status is TaskStatus.AWAITING_USER
            assert persisted_task.project_id is None
            assert naming is not None and naming.status is ProjectNamingStatus.PENDING
            assert len(naming.options) == 9 and naming.revision == 1
            assert queued is not None and queued.status is DeliveryStatus.PENDING

        recovered_options = tuple(
            ProjectNameOption(
                display_name=f"Recovered {index + 1}",
                project_id=f"recovered-{index + 1}",
            )
            for index in range(9)
        )
        recovered_draft = draft.model_copy(update={"project_name_options": recovered_options})
        async with factory.begin() as session:
            recovered_task = await session.get(Task, task_id, with_for_update=True)
            failed_naming = await session.scalar(
                select(ProjectNamingRequest)
                .where(ProjectNamingRequest.task_id == task_id)
                .with_for_update()
            )
            assert recovered_task is not None and failed_naming is not None
            recovered_task.status = TaskStatus.INTERPRETED
            recovered_task.task_draft = recovered_draft.model_dump(mode="json")
            failed_naming.status = ProjectNamingStatus.FAILED
            failed_naming.last_error_category = "provider_unavailable"
            interpretation = Interpretation(
                task_id=recovered_task.id,
                original_input_hash="b" * 64,
                task_draft=recovered_draft.model_dump(mode="json"),
                profile_id="fake",
                model="fake",
                prompt_version="architecture-routing-v4",
                schema_version="1.3",
            )
            session.add(interpretation)
            await session.flush()
            session.add(
                TransactionalOutbox(
                    destination="workflow_dispatch",
                    operation_type="dispatch_interpretation",
                    linked_entity_type="interpretation",
                    linked_entity_id=interpretation.id,
                    idempotency_key=f"workflow:dispatch:{interpretation.id}",
                    payload={"task_id": str(recovered_task.id)},
                )
            )
        assert await dispatcher.process_one()
        async with factory() as session:
            final_task = await session.get(Task, task_id)
            recovered_naming = await session.scalar(
                select(ProjectNamingRequest).where(ProjectNamingRequest.task_id == task_id)
            )
            assert final_task is not None and final_task.status is TaskStatus.AWAITING_USER
            assert (
                recovered_naming is not None
                and recovered_naming.status is ProjectNamingStatus.PENDING
            )
            assert recovered_naming.revision == 2
            assert recovered_naming.last_error_category is None
            assert recovered_naming.options[0]["project_id"] == "recovered-1"
        await engine.dispose()

    asyncio.run(scenario())


def test_provisioner_creates_repository_topic_overlay_and_welcome(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = runtime(tmp_path)
        provisioning_id, outbox_id = await seed_provisioning(factory)
        workspace = FakeWorkspaceClient()
        reloader = FakeReloader()
        service = ProjectProvisioningService(
            configured,
            factory,
            workspace,
            owner="provisioner",
            reloader=reloader,
        )
        assert await service.process_one()
        assert not await service.process_one()
        assert workspace.created == [(-100, "Notes")]
        assert reloader.calls == 1
        repository = configured.settings.repository_root / "notes"
        assert (repository / ".git").is_dir()
        assert (repository / "README.md").read_text().startswith("# Notes")
        overlay_path = configured.settings.registry_overlay_file
        assert overlay_path is not None
        overlay = json.loads(overlay_path.read_text())
        assert overlay["projects"][0]["id"] == "notes"
        assert overlay["topics"][0]["message_thread_id"] == 41
        assert overlay["topics"][0]["default_workflow"] == "adaptive_task"
        async with factory() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            item = await session.get(TransactionalOutbox, outbox_id)
            task = await session.get(Task, row.task_id) if row is not None else None
            assert row is not None and row.status is ProjectProvisioningStatus.COMPLETED
            assert row.topic_thread_id == 41 and row.configuration_revision
            assert task is not None and task.status is TaskStatus.COMPLETED
            assert item is not None and item.status is DeliveryStatus.DELIVERED

        telegram = FakeTelegramClient(next_message_id=80)
        delivery = TelegramDeliveryService(
            factory,
            telegram,
            owner="delivery",
            lease_seconds=30,
            max_attempts=3,
            retry_min_seconds=1,
            retry_max_seconds=10,
        )
        assert await delivery.deliver_one()
        assert len(telegram.sent) == 1
        assert telegram.sent[0][:2] == (-100, 41)
        assert "Notes" in telegram.sent[0][2]
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == "project_welcome"
                )
            )
            assert link is not None and link.message_id == 80
        await engine.dispose()

    asyncio.run(scenario())


def test_topic_unknown_outcome_blocks_without_replay(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = runtime(tmp_path)
        provisioning_id, outbox_id = await seed_provisioning(factory)
        workspace = FakeWorkspaceClient(fail=True)
        service = ProjectProvisioningService(
            configured,
            factory,
            workspace,
            owner="provisioner",
            reloader=FakeReloader(),
        )
        assert await service.process_one()
        assert not await service.process_one()
        assert workspace.created == [(-100, "Notes")]
        async with factory() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            item = await session.get(TransactionalOutbox, outbox_id)
            task = await session.get(Task, row.task_id) if row is not None else None
            assert row is not None and row.status is ProjectProvisioningStatus.BLOCKED
            assert row.last_error_category == "telegram_topic_outcome_unknown"
            assert task is not None and task.status is TaskStatus.BLOCKED
            assert item is not None and item.status is DeliveryStatus.AMBIGUOUS
        await engine.dispose()

    asyncio.run(scenario())


def test_topic_creating_state_is_never_replayed(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = runtime(tmp_path)
        provisioning_id, outbox_id = await seed_provisioning(factory)
        async with factory.begin() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            assert row is not None
            row.status = ProjectProvisioningStatus.TOPIC_CREATING
        workspace = FakeWorkspaceClient()
        service = ProjectProvisioningService(
            configured,
            factory,
            workspace,
            owner="provisioner",
            reloader=FakeReloader(),
        )

        assert await service.process_one()

        assert workspace.created == []
        async with factory() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            item = await session.get(TransactionalOutbox, outbox_id)
            task = await session.get(Task, row.task_id) if row is not None else None
            assert row is not None and row.status is ProjectProvisioningStatus.BLOCKED
            assert task is not None and task.status is TaskStatus.BLOCKED
            assert item is not None and item.status is DeliveryStatus.AMBIGUOUS
        await engine.dispose()

    asyncio.run(scenario())


def test_reload_failure_retries_without_duplicate_topic(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = runtime(tmp_path)
        provisioning_id, _outbox_id = await seed_provisioning(factory)
        workspace = FakeWorkspaceClient()
        reloader = FakeReloader(failures=1)
        service = ProjectProvisioningService(
            configured,
            factory,
            workspace,
            owner="provisioner",
            reloader=reloader,
        )
        assert await service.process_one()
        async with factory.begin() as session:
            await session.execute(
                update(TransactionalOutbox)
                .where(TransactionalOutbox.destination == "project_provisioning")
                .values(available_at=func.now())
            )
        assert await service.process_one()
        assert workspace.created == [(-100, "Notes")]
        assert reloader.calls == 2
        async with factory() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            assert row is not None and row.status is ProjectProvisioningStatus.COMPLETED
        await engine.dispose()

    asyncio.run(scenario())


def test_third_reload_failure_dead_letters_request(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        configured = runtime(tmp_path)
        provisioning_id, outbox_id = await seed_provisioning(factory)
        async with factory.begin() as session:
            item = await session.get(TransactionalOutbox, outbox_id)
            assert item is not None
            item.attempt_count = 2
        service = ProjectProvisioningService(
            configured,
            factory,
            FakeWorkspaceClient(),
            owner="provisioner",
            reloader=FakeReloader(failures=3),
        )

        assert await service.process_one()

        async with factory() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            item = await session.get(TransactionalOutbox, outbox_id)
            task = await session.get(Task, row.task_id) if row is not None else None
            assert row is not None and row.status is ProjectProvisioningStatus.FAILED
            assert row.last_error_category == "oserror"
            assert task is not None and task.status is TaskStatus.FAILED
            assert item is not None and item.status is DeliveryStatus.DEAD_LETTER
        await engine.dispose()

    asyncio.run(scenario())
