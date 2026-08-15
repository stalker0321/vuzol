from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from vuzol.config import Settings
from vuzol.discussion import (
    DomainError,
    PackageControlAction,
    PlanDraft,
    PlanItemDraft,
    WorkPackageService,
)
from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlIngress,
    PackageControlSource,
)
from vuzol.discussion.sequencer import WorkPackageSequenceConsumer
from vuzol.storage.models import MaterializationLink, Task, WorkPackage
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    InteractionMode,
    PlanRevisionCreatedBy,
    TaskStatus,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import enqueue_terminal_task_projections
from vuzol.telegram.work_package_projections import (
    build_work_package_plan_card,
    build_work_package_status_card,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _plan(*, item_ids: tuple[uuid.UUID, uuid.UUID] | None = None) -> PlanDraft:
    return PlanDraft(
        title="MVP package",
        items=tuple(
            PlanItemDraft(
                item_id=None if item_ids is None else item_ids[index - 1],
                local_id=f"step-{index}",
                summary=(
                    "Edited first step" if item_ids is not None and index == 1 else f"Step {index}"
                ),
                goal=f"Deliver goal {index}",
                expected_outcome=f"Outcome {index}",
                completion_criteria=(f"Check {index}",),
                allowed_scope="src/vuzol/**",
            )
            for index in (1, 2)
        ),
    )


def _control(
    action: PackageControlAction,
    *,
    package_id: uuid.UUID,
    revision_number: int,
    content_hash: str,
    generation: int,
    key: str,
) -> AuthoritativeControlCommand:
    return AuthoritativeControlCommand(
        action=action,
        package_id=package_id,
        plan_revision_number=revision_number,
        h8=content_hash[:8],
        expected_status_generation=generation,
        user_id=42,
        source=PackageControlSource.TELEGRAM_CALLBACK,
        external_idempotency_key=key,
    )


async def _complete_with_terminal_outbox(
    factory: async_sessionmaker[AsyncSession], task_id: uuid.UUID
) -> None:
    async with factory.begin() as session:
        task = await session.get(Task, task_id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.COMPLETED
        task.version += 1
        await enqueue_terminal_task_projections(session, task)


async def test_default_off_composed_discussion_to_sequential_completion(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    settings = Settings(environment="test")
    assert settings.project_discussion_enabled is False

    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        await uow.discussions.append_turn(
            session_id=session_id,
            role=ConversationTurnRole.USER,
            source=ConversationTurnSource.TELEGRAM_USER,
            content="Давайте составим план из двух шагов",
            classifier_mode=InteractionMode.DISCUSSION,
        )
        created = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
        revision = await uow.work_packages.get_revision(created.revision_id)
        item_ids = tuple(uuid.UUID(item["item_id"]) for item in revision.immutable_body["items"])
        assert len(item_ids) == 2
        edit = await WorkPackageService(uow).open_edit_session(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            ordinal=1,
            user_id=42,
        )
    async with UnitOfWork(factory) as uow:
        revised = await WorkPackageService(uow).apply_item_edit(
            edit_session_id=edit.id,
            expected_session_generation=1,
            replacement=_plan(item_ids=(item_ids[0], item_ids[1])),
            user_id=42,
        )

    disabled = PackageControlIngress(factory, enabled=False, authorized_user_ids=frozenset({42}))
    with pytest.raises(DomainError, match="project_discussion_disabled"):
        await disabled.apply(
            _control(
                PackageControlAction.APPROVE,
                package_id=created.package_id,
                revision_number=2,
                content_hash=revised.content_hash,
                generation=2,
                key="disabled-approve",
            )
        )

    ingress = PackageControlIngress(factory, enabled=True, authorized_user_ids=frozenset({42}))
    approved = await ingress.apply(
        _control(
            PackageControlAction.APPROVE,
            package_id=created.package_id,
            revision_number=2,
            content_hash=revised.content_hash,
            generation=2,
            key="approve",
        )
    )

    async with factory() as session:
        card = await build_work_package_plan_card(session, created.package_id)
        first_link = await session.scalar(
            select(MaterializationLink).where(
                MaterializationLink.work_package_id == created.package_id,
                MaterializationLink.ordinal == 1,
            )
        )
        assert "Edited first step" in card.html
        assert first_link is not None
        assert await session.scalar(select(func.count()).select_from(Task)) == 1
    await _complete_with_terminal_outbox(factory, first_link.task_id)

    disabled_consumer = WorkPackageSequenceConsumer(
        settings, factory, owner="restart-disabled", enabled=False
    )
    assert not await disabled_consumer.process_one()
    restarted = WorkPackageSequenceConsumer(settings, factory, owner="restart-1", enabled=True)
    assert await restarted.process_one()
    async with factory() as session:
        second_link = await session.scalar(
            select(MaterializationLink).where(
                MaterializationLink.work_package_id == created.package_id,
                MaterializationLink.ordinal == 2,
            )
        )
        assert second_link is not None
        assert await session.scalar(select(func.count()).select_from(Task)) == 2
    await _complete_with_terminal_outbox(factory, second_link.task_id)
    assert await WorkPackageSequenceConsumer(
        settings, factory, owner="restart-2", enabled=True
    ).process_one()
    async with factory() as session:
        package = await session.get(WorkPackage, created.package_id)
        final_card = await build_work_package_status_card(session, created.package_id)
        assert package is not None and package.status is WorkPackageStatus.COMPLETED
        assert package.version > approved.status_generation
        assert final_card.html.startswith("<b>Done | Auto</b>")
        assert await session.scalar(select(func.count()).select_from(Task)) == 2
    await engine.dispose()
