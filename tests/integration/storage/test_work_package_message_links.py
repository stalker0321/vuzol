import hashlib

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from vuzol.storage.models import PlanRevision, Task, TelegramMessageLink, WorkPackage
from vuzol.storage.types import PlanRevisionCreatedBy, PlanRevisionState, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_message_link_persists_button_epoch_without_creating_task(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    content_hash = hashlib.sha256(b"message-link-revision").hexdigest()
    async with UnitOfWork(factory) as uow:
        discussion_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=91
        )
        package = WorkPackage(
            session_id=discussion_id,
            project_id="vuzol",
            status=WorkPackageStatus.DRAFT,
            title="Пакет для карточки",
        )
        package_id = await uow.work_packages.add_package(package)
        revision = PlanRevision(
            work_package_id=package_id,
            revision_number=1,
            state=PlanRevisionState.DRAFT,
            content_hash=content_hash,
            created_by=PlanRevisionCreatedBy.USER,
            immutable_body={"revision": 1},
        )
        revision_id = await uow.work_packages.add_revision(revision)
        package.head_revision_id = revision_id
        await uow.telegram_links.add(
            TelegramMessageLink(
                chat_id=-1001,
                message_thread_id=91,
                message_id=501,
                work_package_id=package_id,
                plan_revision_id=revision_id,
                control_status_generation=package.version,
                message_role="work_package_plan_card",
            )
        )
        await uow.telegram_links.add(
            TelegramMessageLink(
                chat_id=-1001,
                message_thread_id=91,
                message_id=502,
                message_role="legacy_task_card",
            )
        )

    async with UnitOfWork(factory) as uow:
        control = await uow.telegram_links.resolve_work_package_control(-1001, 501)
        legacy = await uow.telegram_links.resolve_work_package_control(-1001, 502)
        session = uow.session
        assert session is not None
        task_count = await session.scalar(select(func.count()).select_from(Task))

    assert control == (package_id, revision_id, 1, "work_package_plan_card")
    assert legacy is None
    assert task_count == 0
    await engine.dispose()


async def test_message_link_rejects_incomplete_or_nonpositive_control_fence(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        discussion_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=92
        )
        package = WorkPackage(session_id=discussion_id, project_id="vuzol", title="Проверка fence")
        package_id = await uow.work_packages.add_package(package)

    with pytest.raises(IntegrityError):
        async with UnitOfWork(factory) as uow:
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-1001,
                    message_thread_id=92,
                    message_id=601,
                    work_package_id=package_id,
                    control_status_generation=0,
                    message_role="invalid_plan_card",
                )
            )
    await engine.dispose()


async def test_topic_allows_only_one_work_package_status_link(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    async with UnitOfWork(factory) as uow:
        await uow.telegram_links.add(
            TelegramMessageLink(
                chat_id=-1001,
                message_thread_id=93,
                message_id=701,
                message_role="work_package_status",
            )
        )
        await uow.telegram_links.add(
            TelegramMessageLink(
                chat_id=-1001,
                message_thread_id=93,
                message_id=702,
                message_role="work_package_plan",
            )
        )
        await uow.telegram_links.add(
            TelegramMessageLink(
                chat_id=-1001,
                message_thread_id=94,
                message_id=703,
                message_role="work_package_status",
            )
        )

    with pytest.raises(IntegrityError):
        async with UnitOfWork(factory) as uow:
            await uow.telegram_links.add(
                TelegramMessageLink(
                    chat_id=-1001,
                    message_thread_id=93,
                    message_id=704,
                    message_role="work_package_status",
                )
            )
    await engine.dispose()
