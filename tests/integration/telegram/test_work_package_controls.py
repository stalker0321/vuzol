from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from vuzol.config import RuntimeConfiguration
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.discussion.service import RevisionResult
from vuzol.interpretation.discussion import DISCUSSION_CLASSIFY_DESTINATION
from vuzol.storage.models import (
    EditSession,
    PlanRevision,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import (
    DeliveryStatus,
    EditSessionStatus,
    PlanRevisionCreatedBy,
    PlanRevisionState,
    WorkPackagePauseReason,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram import TelegramControlService, TelegramIngressService
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import (
    IngressResult,
    IngressStatus,
    MessageUpdate,
    WorkPackageControlUpdate,
)
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.telegram.work_package_projections import (
    WORK_PACKAGE_DETAIL_ROLE,
    WORK_PACKAGE_PLAN_ROLE,
    WORK_PACKAGE_PROJECTION_DESTINATION,
)
from vuzol.telegram.work_packages import (
    ContinueDiscussionOverrides,
    WorkPackageCallback,
    WorkPackageCallbackKind,
    encode_work_package_callback,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _runtime(tmp_path: Path) -> RuntimeConfiguration:
    runtime = telegram_runtime(tmp_path)
    return runtime.model_copy(
        update={
            "settings": runtime.settings.model_copy(update={"project_discussion_enabled": True})
        }
    )


async def _seed(
    factory: async_sessionmaker[AsyncSession],
) -> tuple[RevisionResult, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        result = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=PlanDraft(
                title="Package",
                items=(
                    PlanItemDraft(
                        summary="Implement",
                        goal="Ship",
                        expected_outcome="Done",
                        completion_criteria=("Green",),
                        allowed_scope="src/**",
                    ),
                ),
            ),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
        outbox_id = await uow.outbox.enqueue(
            destination=WORK_PACKAGE_PROJECTION_DESTINATION,
            operation_type="render_plan",
            entity_type="work_package",
            entity_id=result.package_id,
            idempotency_key=f"wp:test:{result.package_id}",
            payload={"package_id": str(result.package_id)},
        )
    return result, outbox_id


async def test_rendered_button_uses_durable_epoch_and_stale_card_fails_closed(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    result, outbox_id = await _seed(factory)
    client = FakeTelegramClient(next_message_id=701)
    delivery = TelegramDeliveryService(
        factory,
        client,
        owner="wp-delivery",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=1,
        retry_max_seconds=10,
    )
    assert await delivery.deliver_one()
    async with factory() as session:
        link = await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.work_package_id == result.package_id,
                TelegramMessageLink.message_role == WORK_PACKAGE_PLAN_ROLE,
            )
        )
        outbox = await session.get(TransactionalOutbox, outbox_id)
    assert link is not None and link.message_id == 701
    assert client.pinned == [(-100, 701)]
    assert link.control_status_generation == 1 and link.plan_revision_id == result.revision_id
    assert outbox is not None and outbox.status is DeliveryStatus.DELIVERED

    data = encode_work_package_callback(
        WorkPackageCallback(
            WorkPackageCallbackKind.APPROVE,
            result.package_id,
            result.revision_number,
            result.content_hash[:8],
        )
    )
    runtime = _runtime(tmp_path)
    overrides = ContinueDiscussionOverrides()
    controls = TelegramControlService(runtime, factory, overrides)
    first = await controls.accept(
        WorkPackageControlUpdate(
            bot_id="main",
            update_id=50,
            callback_query_id="wp-approve-1",
            callback_data=data,
            chat_id=-100,
            message_id=701,
            message_thread_id=10,
            user_id=42,
        )
    )
    stale = await controls.accept(
        WorkPackageControlUpdate(
            bot_id="main",
            update_id=51,
            callback_query_id="wp-approve-stale",
            callback_data=data,
            chat_id=-100,
            message_id=701,
            message_thread_id=10,
            user_id=42,
        )
    )
    stale_open = await controls.accept(
        WorkPackageControlUpdate(
            bot_id="main",
            update_id=52,
            callback_query_id="wp-open-stale",
            callback_data=encode_work_package_callback(
                WorkPackageCallback(
                    WorkPackageCallbackKind.OPEN_ITEM,
                    result.package_id,
                    result.revision_number,
                    result.content_hash[:8],
                    1,
                )
            ),
            chat_id=-100,
            message_id=701,
            message_thread_id=10,
            user_id=42,
        )
    )
    assert first.status is IngressStatus.HANDLED and first.reason == "applied"
    assert stale.status is IngressStatus.REJECTED and stale.reason == "stale_generation"
    assert stale_open.status is IngressStatus.REJECTED and stale_open.reason == "stale_projection"
    async with factory() as session:
        package = await session.get(WorkPackage, result.package_id)
        task_count = await session.scalar(select(func.count()).select_from(Task))
    assert package is not None and package.status is WorkPackageStatus.APPROVED
    assert package.version == 2 and task_count == 0
    await engine.dispose()


async def test_non_mutating_detail_edit_continue_and_clear_lifecycle(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    result, _ = await _seed(factory)
    client = FakeTelegramClient(next_message_id=801)
    delivery = TelegramDeliveryService(
        factory,
        client,
        owner="wp-detail-delivery",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=1,
        retry_max_seconds=10,
    )
    assert await delivery.deliver_one()
    plan_keyboard = client.sent_keyboards[0]
    runtime = _runtime(tmp_path)
    overrides = ContinueDiscussionOverrides()
    controls = TelegramControlService(runtime, factory, overrides)

    async def press(update_id: int, query_id: str, message_id: int, data: str) -> IngressResult:
        return await controls.accept(
            WorkPackageControlUpdate(
                bot_id="main",
                update_id=update_id,
                callback_query_id=query_id,
                callback_data=data,
                chat_id=-100,
                message_id=message_id,
                message_thread_id=10,
                user_id=42,
            )
        )

    open_data = next(data for row in plan_keyboard for label, data in row if label == "1")
    opened = await press(60, "wp-open-1", 801, open_data)
    duplicate = await press(60, "wp-open-1", 801, open_data)
    assert opened.status is IngressStatus.HANDLED and opened.reason == "render_detail"
    assert duplicate.status is IngressStatus.DUPLICATE
    assert await delivery.deliver_one()
    assert client.sent[-1][2].startswith("<b>1. Implement</b>")

    detail_keyboard = client.sent_keyboards[-1]
    edit_data = next(data for row in detail_keyboard for label, data in row if label == "Изменить")
    edited = await press(61, "wp-edit-1", 802, edit_data)
    assert edited.status is IngressStatus.HANDLED and edited.reason == "open_edit"

    discuss_data = next(data for row in plan_keyboard for label, data in row if label == "Обсудить")
    continued = await press(62, "wp-discuss-1", 801, discuss_data)
    assert continued.status is IngressStatus.HANDLED and continued.reason == "continue_discussion"
    ingress = TelegramIngressService(runtime, factory, overrides)
    forced = await ingress.accept_message(
        MessageUpdate(
            bot_id="main",
            update_id=64,
            chat_id=-100,
            message_thread_id=10,
            message_id=900,
            user_id=42,
            text="Let us discuss the tradeoffs",
        )
    )
    normal = await ingress.accept_message(
        MessageUpdate(
            bot_id="main",
            update_id=65,
            chat_id=-100,
            message_thread_id=10,
            message_id=901,
            user_id=42,
            text="One more thought",
        )
    )
    assert forced.status is IngressStatus.HANDLED and normal.status is IngressStatus.HANDLED

    close_data = next(data for row in detail_keyboard for label, data in row if label == "Закрыть")
    closed = await press(63, "wp-close-1", 802, close_data)
    assert closed.status is IngressStatus.HANDLED and closed.reason == "clear_detail"
    assert await delivery.deliver_one()
    assert client.deleted == [(-100, 802)]
    async with factory() as session:
        edit = await session.scalar(select(EditSession))
        detail_link = await session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.work_package_id == result.package_id,
                TelegramMessageLink.message_role == WORK_PACKAGE_DETAIL_ROLE,
            )
        )
        task_count = await session.scalar(select(func.count()).select_from(Task))
        classify_payloads = (
            await session.scalars(
                select(TransactionalOutbox.payload)
                .where(TransactionalOutbox.destination == DISCUSSION_CLASSIFY_DESTINATION)
                .order_by(TransactionalOutbox.created_at)
            )
        ).all()
    assert edit is not None and edit.status is EditSessionStatus.CLOSED
    assert detail_link is None and task_count == 0
    assert [payload["control_override"] for payload in classify_payloads] == [
        "continue_discussion",
        None,
    ]
    await engine.dispose()


async def test_replan_button_waits_for_real_instruction_and_routes_next_turn(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    result, _ = await _seed(factory)
    async with factory.begin() as session:
        package = await session.get(WorkPackage, result.package_id, with_for_update=True)
        revision = await session.get(PlanRevision, result.revision_id, with_for_update=True)
        assert package is not None and revision is not None
        revision.state = PlanRevisionState.APPROVED
        revision.approved_at = datetime.now(UTC)
        revision.approved_by_user_id = 42
        package.status = WorkPackageStatus.RUNNING
        package.approved_revision_id = revision.id
        package.running_revision_id = revision.id
        package.cursor_ordinal = 1
        package.version = 3

    client = FakeTelegramClient(next_message_id=901)
    delivery = TelegramDeliveryService(
        factory,
        client,
        owner="wp-replan-delivery",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=1,
        retry_max_seconds=10,
    )
    assert await delivery.deliver_one()
    runtime = _runtime(tmp_path)
    overrides = ContinueDiscussionOverrides()
    controls = TelegramControlService(runtime, factory, overrides)
    callback = encode_work_package_callback(
        WorkPackageCallback(
            WorkPackageCallbackKind.REQUEST_REPLAN,
            result.package_id,
            result.revision_number,
            result.content_hash[:8],
        )
    )

    replanning = await controls.accept(
        WorkPackageControlUpdate(
            bot_id="main",
            update_id=70,
            callback_query_id="wp-replan-real",
            callback_data=callback,
            chat_id=-100,
            message_id=901,
            message_thread_id=10,
            user_id=42,
        )
    )
    assert replanning.status is IngressStatus.HANDLED
    assert replanning.reason == "request_replan"

    ingress = TelegramIngressService(runtime, factory, overrides)
    message = await ingress.accept_message(
        MessageUpdate(
            bot_id="main",
            update_id=71,
            chat_id=-100,
            message_thread_id=10,
            message_id=902,
            user_id=42,
            text="Оставь готовый первый пункт и раздели оставшуюся работу на два шага",
        )
    )
    assert message.status is IngressStatus.HANDLED

    async with factory() as session:
        package = await session.get(WorkPackage, result.package_id)
        revisions = tuple(
            (
                await session.scalars(
                    select(PlanRevision).where(PlanRevision.work_package_id == result.package_id)
                )
            ).all()
        )
        classify = await session.scalar(
            select(TransactionalOutbox)
            .where(TransactionalOutbox.destination == DISCUSSION_CLASSIFY_DESTINATION)
            .order_by(TransactionalOutbox.created_at.desc())
            .limit(1)
        )
    assert package is not None and package.status is WorkPackageStatus.PAUSED
    assert package.pause_reason is WorkPackagePauseReason.REPLAN_REQUIRED
    assert len(revisions) == 1
    assert classify is not None and classify.payload["control_override"] == "replan"
    await engine.dispose()
