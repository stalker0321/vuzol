import asyncio
import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select

from vuzol.interpretation.domain import (
    ProjectNameOption,
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.models import (
    ExternalInbox,
    ProjectNamingRequest,
    ProjectProvisioning,
    Task,
    TelegramControlAction,
    TelegramIntakeMessage,
    TransactionalOutbox,
)
from vuzol.storage.types import IntakeStatus, ProjectNamingStatus, RiskLevel, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram import TelegramControlService
from vuzol.telegram.domain import ControlUpdate, IngressStatus

from ..storage.helpers import storage
from .helpers import telegram_runtime


def naming_draft() -> TaskDraft:
    return TaskDraft(
        action=TaskAction.CREATE_PROJECT,
        task_type=TaskType.INFRASTRUCTURE,
        operation=TaskOperation.CREATE,
        goal="A private note-taking application",
        task_summary="Create a private note-taking application",
        project_name_options=tuple(
            ProjectNameOption(display_name=f"Notes {index + 1}", project_id=f"notes-{index + 1}")
            for index in range(9)
        ),
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=RiskLevel.LOW,
        needs_planning=False,
        needs_clarification=False,
        normalized_title="Private notes",
    )


async def seed_naming(factory: object) -> ProjectNamingRequest:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    draft = naming_draft()
    async with typed.begin() as session:
        task = Task(
            user_id=42,
            source_chat_id=-100,
            source_thread_id=10,
            original_text=draft.goal,
            task_type="infrastructure",
            task_draft=draft.model_dump(mode="json"),
            status=TaskStatus.AWAITING_USER,
        )
        session.add(task)
        await session.flush()
        inbox = ExternalInbox(
            source="telegram",
            consumer="bot:main",
            external_event_id=f"naming-{task.id}",
            payload_hash="c" * 64,
        )
        session.add(inbox)
        await session.flush()
        session.add(
            TelegramIntakeMessage(
                inbox_id=inbox.id,
                chat_id=-100,
                message_thread_id=10,
                message_id=400,
                user_id=42,
                task_id=task.id,
                original_text=draft.goal,
                affinity_kind="new_task",
                status=IntakeStatus.AWAITING_INTERPRETATION,
            )
        )
        naming = ProjectNamingRequest(
            task_id=task.id,
            requested_by_user_id=42,
            chat_id=-100,
            source_thread_id=10,
            description=draft.goal,
            options=[option.model_dump(mode="json") for option in draft.project_name_options],
            revision=1,
            status=ProjectNamingStatus.PENDING,
        )
        session.add(naming)
        await session.flush()
        naming_id = naming.id
    async with typed() as session:
        persisted = await session.get(ProjectNamingRequest, naming_id)
        assert persisted is not None
        return persisted


@pytest.mark.postgresql
def test_callback_is_persisted_idempotently_without_executing_action(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                original_text="task",
                task_type="general",
            )
        update = ControlUpdate(
            bot_id="main",
            update_id=20,
            callback_query_id="callback-1",
            chat_id=-100,
            user_id=42,
            action_kind="cancel",
            task_id=task.id,
        )
        service = TelegramControlService(telegram_runtime(tmp_path), factory)
        first = await service.accept(update)
        duplicate = await service.accept(update)
        assert first.status is IngressStatus.CREATED
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            assert (
                await session.scalar(select(func.count()).select_from(TelegramControlAction)) == 1
            )
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_control_rejects_unauthorized_or_missing_target(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramControlService(telegram_runtime(tmp_path), factory)
        unauthorized = await service.accept(
            ControlUpdate(
                bot_id="main",
                update_id=21,
                callback_query_id="callback-2",
                chat_id=-100,
                user_id=999,
                action_kind="cancel",
                task_id=uuid.uuid4(),
            )
        )
        missing = await service.accept(
            ControlUpdate(
                bot_id="main",
                update_id=22,
                callback_query_id="callback-3",
                chat_id=-100,
                user_id=42,
                action_kind="cancel",
                task_id=uuid.uuid4(),
            )
        )
        assert unauthorized.status is IngressStatus.REJECTED
        assert missing.status is IngressStatus.REJECTED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_name_selection_starts_provisioning(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        naming = await seed_naming(factory)
        service = TelegramControlService(telegram_runtime(tmp_path), factory)
        result = await service.accept(
            ControlUpdate(
                bot_id="main",
                update_id=30,
                callback_query_id="name-1",
                chat_id=-100,
                user_id=42,
                action_kind="project_name_select",
                naming_request_id=naming.id,
                naming_revision=1,
                naming_option_index=0,
            )
        )
        assert result.status is IngressStatus.CREATED
        async with factory() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id)
            task = await session.get(Task, naming.task_id)
            provisioning = await session.scalar(
                select(ProjectProvisioning).where(ProjectProvisioning.task_id == naming.task_id)
            )
            operations = set(await session.scalars(select(TransactionalOutbox.operation_type)))
            assert persisted is not None and persisted.status is ProjectNamingStatus.SELECTED
            assert persisted.selected_project_id == "notes-1"
            assert task is not None and task.status is TaskStatus.EXECUTING
            assert task.project_id == "notes-1"
            assert provisioning is not None and provisioning.project_id == "notes-1"
            assert operations == {"delete_message", "create_project", "send_message"}
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_name_regeneration_invalidates_old_buttons(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        naming = await seed_naming(factory)
        service = TelegramControlService(telegram_runtime(tmp_path), factory)
        regenerate = ControlUpdate(
            bot_id="main",
            update_id=31,
            callback_query_id="regenerate-1",
            chat_id=-100,
            user_id=42,
            action_kind="project_name_regenerate",
            naming_request_id=naming.id,
            naming_revision=1,
        )
        assert (await service.accept(regenerate)).status is IngressStatus.CREATED
        stale = regenerate.model_copy(
            update={
                "update_id": 32,
                "callback_query_id": "stale",
                "action_kind": "project_name_select",
                "naming_option_index": 1,
            }
        )
        rejected = await service.accept(stale)
        assert rejected.status is IngressStatus.REJECTED
        async with factory() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id)
            operations = set(
                await session.scalars(
                    select(TransactionalOutbox.operation_type).where(
                        TransactionalOutbox.linked_entity_id == naming.id
                    )
                )
            )
            assert persisted is not None and persisted.status is ProjectNamingStatus.GENERATING
            assert persisted.revision == 2
            assert operations == {"delete_message", "regenerate_project_names"}
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_naming_rejects_stale_invalid_and_conflicting_choices(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        naming = await seed_naming(factory)
        service = TelegramControlService(telegram_runtime(tmp_path), factory)

        def selection(update_id: int, **changes: object) -> ControlUpdate:
            values: dict[str, object] = {
                "bot_id": "main",
                "update_id": update_id,
                "callback_query_id": f"rejected-{update_id}",
                "chat_id": -100,
                "user_id": 42,
                "action_kind": "project_name_select",
                "naming_request_id": naming.id,
                "naming_revision": 1,
                "naming_option_index": 0,
            }
            values.update(changes)
            return ControlUpdate.model_validate(values)

        missing = await service.accept(selection(40, naming_request_id=uuid.uuid4()))
        stale = await service.accept(selection(41, naming_revision=2))
        no_option = await service.accept(selection(42, naming_option_index=None))
        no_target = await service.accept(selection(46, naming_request_id=None))
        assert {missing.status, stale.status, no_option.status, no_target.status} == {
            IngressStatus.REJECTED
        }

        async with factory.begin() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id, with_for_update=True)
            assert persisted is not None
            options = list(persisted.options)
            options[0] = {"display_name": "Vuzol Again", "project_id": "vuzol"}
            persisted.options = options
        conflict = await service.accept(selection(43))
        assert conflict.status is IngressStatus.REJECTED

        async with factory.begin() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id, with_for_update=True)
            assert persisted is not None
            persisted.options = []
        invalid_option = await service.accept(selection(47))
        assert invalid_option.status is IngressStatus.REJECTED

        async with factory.begin() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id, with_for_update=True)
            task = await session.get(Task, naming.task_id, with_for_update=True)
            assert persisted is not None and task is not None
            persisted.options = [
                option.model_dump(mode="json") for option in naming_draft().project_name_options
            ]
            task.status = TaskStatus.COMPLETED
        wrong_task_state = await service.accept(selection(48))
        assert wrong_task_state.status is IngressStatus.REJECTED

        async with factory.begin() as session:
            task = await session.get(Task, naming.task_id, with_for_update=True)
            assert task is not None
            task.status = TaskStatus.AWAITING_USER
            session.add(
                ProjectProvisioning(
                    task_id=task.id,
                    requested_by_user_id=42,
                    chat_id=-100,
                    source_thread_id=10,
                    project_id="already-provisioning",
                    display_name="Already Provisioning",
                    description="Existing request",
                    repository_path="already-provisioning",
                )
            )
        duplicate_provisioning = await service.accept(selection(49))
        assert duplicate_provisioning.status is IngressStatus.REJECTED

        async with factory.begin() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id, with_for_update=True)
            assert persisted is not None
            persisted.requested_by_user_id = 41
        wrong_author = await service.accept(selection(44))
        assert wrong_author.status is IngressStatus.REJECTED

        async with factory.begin() as session:
            persisted = await session.get(ProjectNamingRequest, naming.id, with_for_update=True)
            assert persisted is not None
            persisted.requested_by_user_id = 42
            persisted.status = ProjectNamingStatus.GENERATING
        not_pending = await service.accept(selection(45))
        assert not_pending.status is IngressStatus.REJECTED
        await engine.dispose()

    asyncio.run(scenario())
