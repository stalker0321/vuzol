import asyncio
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import func, select

from vuzol.config import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
    RegistryDocument,
    RuntimeConfiguration,
    TopicConfig,
    TopicKind,
    build_bundle,
)
from vuzol.storage.models import (
    ExternalInbox,
    Run,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram import TelegramIngressService
from vuzol.telegram.dogfood import TelegramDogfoodIngressService
from vuzol.telegram.domain import IngressStatus, MessageUpdate

from ..storage.helpers import storage
from .helpers import telegram_runtime


def initialize_repository(repository: Path) -> None:
    subprocess.run(("git", "init", "-b", "main", str(repository)), check=True, capture_output=True)
    subprocess.run(
        ("git", "-C", str(repository), "config", "user.email", "test@example.invalid"),
        check=True,
    )
    subprocess.run(("git", "-C", str(repository), "config", "user.name", "Test"), check=True)
    (repository / "README.md").write_text("base\n")
    subprocess.run(("git", "-C", str(repository), "add", "."), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "commit", "-m", "base"),
        check=True,
        capture_output=True,
    )


def message(update_id: int, message_id: int, **changes: object) -> MessageUpdate:
    values: dict[str, object] = {
        "bot_id": "main",
        "update_id": update_id,
        "chat_id": -100,
        "message_thread_id": 10,
        "message_id": message_id,
        "user_id": 42,
        "text": "create a task",
    }
    values.update(changes)
    return MessageUpdate.model_validate(values)


@pytest.mark.postgresql
def test_authorized_project_intake_is_atomic_and_duplicate_safe(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(telegram_runtime(tmp_path), factory)
        first = await service.accept_message(message(1, 100))
        duplicate = await service.accept_message(message(1, 100))

        assert first.status is IngressStatus.CREATED and first.task_id is not None
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            task = await session.get(Task, first.task_id)
            assert task is not None
            assert task.topic_task_number == 1
            assert task.public_task_number == 100001
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
            assert await session.scalar(select(func.count()).select_from(TopicMapping)) == 1
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 2
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_topic_task_numbers_are_atomic_and_independent(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)

        async def create(thread_id: int) -> tuple[int | None, int | None]:
            async with UnitOfWork(factory) as uow:
                task = await uow.tasks.create(
                    user_id=42,
                    chat_id=-100,
                    thread_id=thread_id,
                    project_id="vuzol",
                    original_text="task",
                    task_type="coding",
                )
                return task.topic_task_number, task.public_task_number

        first, second = await asyncio.gather(create(73), create(73))
        other = await create(74)

        assert sorted((first, second)) == [(1, 730001), (2, 730002)]
        assert other == (1, 740001)
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_explicit_sol_command_seeds_durable_worker_trial_once(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        runtime = telegram_runtime(tmp_path)
        initialize_repository(runtime.settings.repository_root / "vuzol")
        profile = ProviderProfileConfig(
            id="codex-subscription-prod",
            provider="codex",
            model="codex",
            launch_mode=LaunchMode.CLI,
            credential_required=False,
            capabilities=frozenset(
                {
                    Capability.REPOSITORY_READ,
                    Capability.CODE_EDIT,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            concurrency_limit=1,
            cost_class=CostClass.STRONG,
            roles=frozenset({ProviderRole.EXECUTOR}),
            supported_task_types=frozenset({"coding"}),
            runtime_identity="codex-a",
            state_directory=tmp_path / "codex-state",
        )
        project = runtime.registries.projects.get("vuzol")
        sandbox = runtime.registries.sandboxes.get("project-default")
        document = RegistryDocument(
            projects=(project,),
            profiles=(profile,),
            topics=(
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=10,
                    kind=TopicKind.PROJECT,
                    project_id="vuzol",
                    default_workflow="adaptive_worker_trial",
                ),
            ),
            sandboxes=(sandbox,),
        )
        dogfood_runtime = RuntimeConfiguration(
            settings=runtime.settings,
            registries=build_bundle(document, runtime.settings, validate_profile_credentials=False),
        )
        engine, factory = storage(postgres_dsn)
        service = TelegramDogfoodIngressService(dogfood_runtime, factory)
        update = message(
            1,
            100,
            text="/sol README.md tests/unit/test_readme.py\nAdd the bounded status example.",
        )
        first = await service.accept_message(update)
        duplicate = await service.accept_message(update)

        assert first is not None
        assert duplicate is not None
        assert first.status is IngressStatus.CREATED and first.task_id is not None
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            task = await session.get(Task, first.task_id)
            run = await session.scalar(select(Run).where(Run.task_id == first.task_id))
            assert task is not None
            assert (task.user_id, task.source_chat_id, task.source_thread_id) == (42, -100, 10)
            assert run is not None and run.workflow_type == "adaptive_worker_trial"
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_unauthorized_input_creates_no_persisted_content(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(telegram_runtime(tmp_path), factory)
        result = await service.accept_message(message(1, 100, user_id=999))
        assert result.status is IngressStatus.REJECTED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 0
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_only_reply_has_affinity_and_standalone_message_creates_task(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        async with UnitOfWork(factory) as uow:
            task_a = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="task A",
                task_type="general",
            )
            await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="task B",
                task_type="general",
            )
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=50,
                    task_id=task_a.id,
                    message_role="status_card",
                )
            )
        service = TelegramIngressService(runtime, factory)
        reply = await service.accept_message(message(2, 101, reply_to_message_id=50))
        standalone = await service.accept_message(message(3, 102))

        assert reply.status is IngressStatus.CONTINUATION and reply.task_id == task_a.id
        assert standalone.status is IngressStatus.CREATED
        assert standalone.task_id is not None and standalone.task_id != task_a.id
        async with factory() as session:
            intake = await session.scalar(
                select(TelegramIntakeMessage).where(
                    TelegramIntakeMessage.id == standalone.intake_id
                )
            )
            assert intake is not None
            assert intake.affinity_kind == "new_task"
            assert intake.ambiguous_task_ids == []
            assert await session.scalar(select(func.count()).select_from(Task)) == 3
        await engine.dispose()

    asyncio.run(scenario())
