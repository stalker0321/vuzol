import asyncio
import subprocess
from pathlib import Path

import pytest
from pydantic import HttpUrl
from sqlalchemy import func, select

from vuzol.config import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
    RegistryDocument,
    RuntimeConfiguration,
    Settings,
    TopicConfig,
    TopicKind,
    build_bundle,
)
from vuzol.config.settings import SecretIngressSettings
from vuzol.storage.models import (
    ExternalInbox,
    ProjectDiscussionSession,
    ProjectProvisioning,
    Run,
    SecretIngressRequest,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.types import TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram import TelegramIngressService
from vuzol.telegram.delivery import (
    PermanentDeliveryError,
    TelegramDeliveryService,
    prepare_delivery,
)
from vuzol.telegram.dogfood import TelegramDogfoodIngressService
from vuzol.telegram.domain import (
    AttachmentKind,
    IngressStatus,
    MessageUpdate,
    TelegramAttachment,
)
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.telegram.work_package_projections import WORK_PACKAGE_STATUS_ROLE

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


def inbox_runtime(tmp_path: Path) -> RuntimeConfiguration:
    configured = telegram_runtime(tmp_path)
    document = RegistryDocument(
        projects=configured.registries.projects.items(),
        topics=(
            TopicConfig(
                chat_id=-100,
                message_thread_id=10,
                kind=TopicKind.INBOX,
                accepts_new_tasks=True,
                default_workflow="simple_model_task",
            ),
        ),
        sandboxes=configured.registries.sandboxes.items(),
    )
    return RuntimeConfiguration(
        settings=configured.settings,
        registries=build_bundle(document, configured.settings),
    )


@pytest.mark.postgresql
def test_import_command_collects_url_and_queues_existing_repository(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(inbox_runtime(tmp_path), factory)

        prompted = await service.accept_message(message(701, 801, text="/import"))
        imported = await service.accept_message(
            message(702, 802, text="https://github.com/example/three-body-problems")
        )

        assert prompted.status is IngressStatus.HANDLED
        assert imported.status is IngressStatus.HANDLED
        assert imported.task_id == prompted.task_id
        async with factory() as session:
            task = await session.get(Task, prompted.task_id)
            provisioning = await session.scalar(select(ProjectProvisioning))
            assert task is not None and task.status is TaskStatus.EXECUTING
            assert task.project_id == "three-body-problems"
            assert provisioning is not None
            assert provisioning.project_id == "three-body-problems"
            assert provisioning.source_repository_url == (
                "https://github.com/example/three-body-problems.git"
            )
            queued = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "project_provisioning"
                )
            )
            assert queued is not None and queued.operation_type == "import_project"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_import_rejects_non_github_source_without_consuming_request(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(inbox_runtime(tmp_path), factory)
        prompted = await service.accept_message(message(711, 811, text="/import"))
        rejected = await service.accept_message(
            message(712, 812, text="file:///opt/three-body-problem")
        )
        assert prompted.status is IngressStatus.HANDLED
        assert rejected.status is IngressStatus.REJECTED
        async with factory() as session:
            task = await session.get(Task, prompted.task_id)
            assert task is not None and task.status is TaskStatus.AWAITING_USER
            assert await session.scalar(select(ProjectProvisioning.id)) is None
        await engine.dispose()

    asyncio.run(scenario())


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
def test_secret_command_is_system_only_and_never_persists_a_value(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        settings = Settings(
            environment="test",
            allowed_user_ids=(42,),
            allowed_chat_ids=(-100,),
            repository_root=tmp_path / "repositories",
            artifact_root=tmp_path / "artifacts",
            secret_file_root=tmp_path / "secrets",
            secret_ingress=SecretIngressSettings(
                enabled=True,
                public_base_url=HttpUrl("https://vuzol.example"),
                storage_root=tmp_path / "managed-secrets",
                allowed_names=("TOKENROUTER_API_KEY",),
            ),
        )
        document = RegistryDocument(
            topics=(
                TopicConfig(
                    chat_id=-100,
                    message_thread_id=10,
                    kind=TopicKind.SYSTEM,
                    accepts_new_tasks=False,
                    default_workflow="simple_model_task",
                ),
            )
        )
        runtime = RuntimeConfiguration(
            settings=settings, registries=build_bundle(document, settings)
        )
        result = await TelegramIngressService(runtime, factory).accept_message(
            message(101, 201, text="/secret TOKENROUTER_API_KEY")
        )
        assert result.status is IngressStatus.HANDLED
        async with factory() as session:
            request = await session.scalar(select(SecretIngressRequest))
            outbox = (await session.scalars(select(TransactionalOutbox))).all()
            assert request is not None
            assert request.secret_name == "TOKENROUTER_API_KEY"  # noqa: S105  # pragma: allowlist secret
            assert len(outbox) == 2
            serialized = " ".join(str(item.payload) for item in outbox)
            assert "TOKENROUTER_API_KEY=" not in serialized
        await engine.dispose()

    asyncio.run(scenario())


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
def test_enabled_project_discussion_forks_before_task_creation(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(update={"project_discussion_enabled": True})
            }
        )
        service = TelegramIngressService(runtime, factory)
        first = await service.accept_message(message(501, 601, text="есть идея по интерфейсу"))
        duplicate = await service.accept_message(message(501, 601, text="есть идея по интерфейсу"))

        assert first.status is IngressStatus.HANDLED
        assert first.task_id is None and first.intake_id is not None
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(ProjectDiscussionSession))
                == 1
            )
            intake = await session.get(TelegramIntakeMessage, first.intake_id)
            assert intake is not None
            assert intake.task_id is None
            assert intake.affinity_kind == "discussion"
            items = (
                await session.scalars(
                    select(TransactionalOutbox).order_by(TransactionalOutbox.created_at)
                )
            ).all()
            assert [(item.destination, item.operation_type) for item in items] == [
                ("discussion_classify", "classify_intake"),
                ("work_package_projection", "render_topic_status"),
            ]
            assert items[0].payload["project_id"] == "vuzol"
        client = FakeTelegramClient(next_message_id=777)
        delivery = TelegramDeliveryService(
            factory,
            client,
            owner="topic-status-delivery",
            lease_seconds=30,
            max_attempts=3,
            retry_min_seconds=1,
            retry_max_seconds=10,
        )
        assert await delivery.deliver_one()
        assert client.pinned == [(-100, 777)]
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == WORK_PACKAGE_STATUS_ROLE
                )
            )
            assert link is not None and link.message_id == 777
            assert link.work_package_id is None
            mapping = await session.scalar(select(TopicMapping))
            assert mapping is not None
            invalid_entity = TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_topic_status",
                linked_entity_type="work_package",
                linked_entity_id=mapping.id,
                idempotency_key="topic-status:invalid-entity",
                payload={},
            )
            with pytest.raises(PermanentDeliveryError, match="invalid_topic_status_entity"):
                await prepare_delivery(session, invalid_entity)
            mismatched = TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_topic_status",
                linked_entity_type="topic_mapping",
                linked_entity_id=mapping.id,
                idempotency_key="topic-status:mismatch",
                payload={
                    "chat_id": -100,
                    "thread_id": 10,
                    "project_id": "another-project",
                    "revision": 1,
                },
            )
            with pytest.raises(PermanentDeliveryError, match="topic_status_mapping_mismatch"):
                await prepare_delivery(session, mismatched)
        async with factory.begin() as session:
            mapping = await session.scalar(select(TopicMapping))
            assert mapping is not None
            session.add(
                TransactionalOutbox(
                    destination="work_package_projection",
                    operation_type="render_topic_status",
                    linked_entity_type="topic_mapping",
                    linked_entity_id=mapping.id,
                    idempotency_key="topic-status:stale",
                    payload={
                        "chat_id": -100,
                        "thread_id": 10,
                        "project_id": "vuzol",
                        "revision": 600,
                    },
                )
            )
        assert await delivery.deliver_one()
        assert client.edited == []
        follow_up = await service.accept_message(message(502, 602, text="ещё одна деталь идеи"))
        assert follow_up.status is IngressStatus.HANDLED
        async with factory() as session:
            bootstraps = await session.scalar(
                select(func.count())
                .select_from(TransactionalOutbox)
                .where(TransactionalOutbox.operation_type == "render_topic_status")
            )
            assert bootstraps == 2
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_project_discussion_reply_requires_canonical_task_affinity(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        base = telegram_runtime(tmp_path)
        runtime = base.model_copy(
            update={
                "settings": base.settings.model_copy(update={"project_discussion_enabled": True})
            }
        )
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="existing task",
                task_type="general",
            )
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=700,
                    task_id=task.id,
                    message_role="task_status",
                )
            )

        service = TelegramIngressService(runtime, factory)
        topic_root_reply = await service.accept_message(
            message(502, 602, text="обсудим идею", reply_to_message_id=699)
        )
        task_reply = await service.accept_message(
            message(503, 603, text="продолжай задачу", reply_to_message_id=700)
        )

        assert topic_root_reply.status is IngressStatus.HANDLED
        assert topic_root_reply.task_id is None
        assert task_reply.status is IngressStatus.CONTINUATION
        assert task_reply.task_id == task.id
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(ProjectDiscussionSession))
                == 1
            )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_concurrent_first_discussion_messages_share_one_session(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        runtime = runtime.model_copy(
            update={
                "settings": runtime.settings.model_copy(update={"project_discussion_enabled": True})
            }
        )
        service = TelegramIngressService(runtime, factory)

        first, second = await asyncio.gather(
            service.accept_message(message(510, 610, text="первая идея")),
            service.accept_message(message(511, 611, text="вторая идея")),
        )

        assert first.status is IngressStatus.HANDLED
        assert second.status is IngressStatus.HANDLED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(ProjectDiscussionSession))
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(TelegramIntakeMessage)) == 2
            )
            assert await session.scalar(select(func.count()).select_from(TransactionalOutbox)) == 3
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_help_is_handled_once_without_creating_a_task(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(telegram_runtime(tmp_path), factory)
        update = message(2, 101, text="/help@vuzol_bot")

        first = await service.accept_message(update)
        duplicate = await service.accept_message(update)

        assert first.status is IngressStatus.HANDLED
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
            items = (
                await session.scalars(
                    select(TransactionalOutbox).order_by(TransactionalOutbox.created_at)
                )
            ).all()
            assert [(item.operation_type, item.payload["role"]) for item in items] == [
                ("send_message", "help_card"),
                ("delete_message", "user_command_delete"),
            ]
            assert items[0].payload["topic_kind"] == "project"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
@pytest.mark.parametrize(
    ("content_kind", "expected_outbox_count"),
    (("text", 2), ("voice", 2), ("document", 3)),
)
def test_text_voice_and_document_intake_are_duplicate_safe(
    postgres_dsn: str,
    tmp_path: Path,
    content_kind: str,
    expected_outbox_count: int,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        service = TelegramIngressService(telegram_runtime(tmp_path), factory)
        attachments: tuple[TelegramAttachment, ...] = ()
        text: str | None = "create a task"
        if content_kind != "text":
            attachment_kind = (
                AttachmentKind.VOICE if content_kind == "voice" else AttachmentKind.DOCUMENT
            )
            attachments = (
                TelegramAttachment(
                    file_id=f"{content_kind}-file",
                    file_unique_id=f"{content_kind}-unique",
                    kind=attachment_kind,
                    file_size=64,
                    media_type="audio/ogg" if content_kind == "voice" else "text/plain",
                    filename=None if content_kind == "voice" else "request.txt",
                ),
            )
            text = None
        update = message(50, 150, text=text, attachments=attachments)

        first = await service.accept_message(update)
        duplicate = await service.accept_message(update)

        assert first.status is IngressStatus.CREATED and first.task_id is not None
        assert duplicate.status is IngressStatus.DUPLICATE
        async with factory() as session:
            intake = await session.get(TelegramIntakeMessage, first.intake_id)
            assert intake is not None
            assert len(intake.attachments) == len(attachments)
            if attachments:
                assert intake.attachments[0]["kind"] == content_kind
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            assert await session.scalar(select(func.count()).select_from(ExternalInbox)) == 1
            intake_count = await session.scalar(
                select(func.count()).select_from(TelegramIntakeMessage)
            )
            assert intake_count == 1
            assert (
                await session.scalar(select(func.count()).select_from(TransactionalOutbox))
                == expected_outbox_count
            )
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
