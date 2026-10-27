"""Package-task cards: owner buttons use the card revision."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from telegram import Update

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    PlanRevisionItem,
    Step,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import (
    IdempotencyClass,
    PlanRevisionCreatedBy,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.adapter import control_update
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import ControlUpdate, IngressStatus
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.controls import WorkflowControlConsumer

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]

_KEEPS = "saved work remains after process restart and database reconnect"
_LOGS = "logs stay readable after reconnect"


def _plan() -> PlanDraft:
    return PlanDraft(
        title="Ship the feature",
        items=(
            PlanItemDraft(
                local_id="item-1",
                summary="Step 1",
                goal="Goal 2",
                expected_outcome="Outcome 1",
                completion_criteria=("Check 1",),
                allowed_scope="src/**",
            ),
            PlanItemDraft(
                local_id="item-2",
                summary="Step 2",
                goal="Goal 2",
                expected_outcome="Outcome 2",
                completion_criteria=("Check 2",),
                allowed_scope="src/**",
            ),
        ),
    )


async def _materialize(factory) -> tuple[uuid.UUID, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=91
        )
        result = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
        assert uow.session is not None
        task = await uow.tasks.create(
            user_id=42,
            chat_id=-100,
            thread_id=10,
            project_id="vuzol",
            original_text="plan item",
            task_type="coding",
        )
        item = await uow.session.scalar(
            select(PlanRevisionItem).where(
                PlanRevisionItem.plan_revision_id == result.revision_id,
                PlanRevisionItem.ordinal == 1,
            )
        )
        assert item is not None
        await uow.work_packages.add_materialization(
            MaterializationLink(
                work_package_id=result.package_id,
                plan_revision_id=result.revision_id,
                work_item_draft_id=item.item_id,
                plan_revision_item_id=item.id,
                task_id=task.id,
                ordinal=1,
            )
        )
    return task.id, result.package_id


async def _block(
    factory,
    task_id: uuid.UUID,
    package_id: uuid.UUID,
    *,
    category: str,
    result_commit: str | None = None,
) -> int:
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        task = await uow.session.get(Task, task_id)
        package = await uow.session.get(WorkPackage, package_id)
        assert task is not None and package is not None
        task.version = 4
        task.status = TaskStatus.BLOCKED
        package.version = 4
        run_id = await uow.runs.create(
            task_id=task.id,
            workflow_type="coding",
            workflow_version="1",
            budget_mode="balanced",
            configuration_revision="a" * 64,
            policy_revision="b" * 64,
            status=RunStatus.BLOCKED,
        )
        step = await uow.steps.create(
            run_id=run_id,
            ordinal=2,
            step_type="review",
            idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
            status=StepStatus.BLOCKED,
            max_attempts=1,
        )
        stored = await uow.session.get(Step, step.id)
        assert stored is not None
        stored.failure_category = category
        stored.failure_summary = "owner action required"
        if result_commit is not None:
            uow.session.add(
                Worktree(
                    task_id=task.id,
                    run_id=run_id,
                    project_id="vuzol",
                    repository_identity_hash="c" * 64,
                    base_commit="a" * 40,
                    default_branch="main",
                    expected_target_head="a" * 40,
                    branch=f"kept-{task.id.hex[:8]}",
                    path=f"worktrees/owner-{task.id.hex}",
                    owner="test",
                    delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                    result_commit=result_commit,
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        return stored.ordinal


def _delivery(factory, runtime, client: FakeTelegramClient) -> TelegramDeliveryService:
    return TelegramDeliveryService(
        factory,
        client,
        owner="delivery",
        lease_seconds=30,
        max_attempts=2,
        retry_min_seconds=1,
        retry_max_seconds=2,
        topics=runtime.registries.topics,
        projects=runtime.registries.projects,
    )


async def _deliver(
    factory,
    runtime,
    task_id: uuid.UUID,
    client: FakeTelegramClient | None = None,
) -> FakeTelegramClient:
    async with factory.begin() as session:
        session.add(
            TransactionalOutbox(
                destination="telegram",
                operation_type="send_message",
                linked_entity_type="task",
                linked_entity_id=task_id,
                idempotency_key=f"test:owner-card:{task_id}",
                payload={
                    "chat_id": -100,
                    "message_thread_id": 10,
                    "role": "intake_ack",
                    "task_id": str(task_id),
                },
            )
        )
    telegram = client or FakeTelegramClient(next_message_id=77)
    assert await _delivery(factory, runtime, telegram).deliver_one()
    return telegram


def _press(data: str, update_id: int) -> ControlUpdate:
    converted = control_update(
        Update.de_json(
            {
                "update_id": update_id,
                "callback_query": {
                    "id": f"callback-{update_id}",
                    "from": {"id": 42, "is_bot": False, "first_name": "Owner"},
                    "chat_instance": "test",
                    "data": data,
                    "message": {
                        "message_id": 77,
                        "message_thread_id": 10,
                        "date": 0,
                        "chat": {"id": -100, "type": "supergroup"},
                    },
                },
            },
            None,
        ),
        "main",
    )
    assert isinstance(converted, ControlUpdate)
    return converted


def _flat(telegram: FakeTelegramClient) -> list[tuple[str, str]]:
    return [(label, data) for row in telegram.sent_keyboards[0] for label, data in row]


async def test_package_card_owner_button_uses_the_card_revision(postgres_dsn, tmp_path) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        runtime = telegram_runtime(tmp_path)
        task_id, package_id = await _materialize(factory)
        await _block(
            factory,
            task_id,
            package_id,
            category="admission_only_consent_required",
        )
        telegram = await _deliver(factory, runtime, task_id)
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.task_id == task_id)
            )
            assert link is not None
            assert link.projection_revision == 40004
            assert link.message_id == 77
        data = next(data for _label, data in _flat(telegram) if data.startswith("v2:admit:"))
        assert data.split(":")[3] == "40004"
        service = TelegramControlService(runtime, factory)
        forged = data.split(":")
        forged[3] = "4"
        rejected = await service.accept(_press(":".join(forged), 1))
        assert rejected.status is IngressStatus.REJECTED
        assert rejected.reason == "task_retry_stale"
        async with factory() as session:
            step = await session.scalar(select(Step).where(Step.ordinal == 2))
            assert step is not None and step.status is StepStatus.BLOCKED
            consent = await session.scalar(
                select(Event.id).where(Event.event_type == "budget.admission_only_consent")
            )
            assert consent is None
        accepted = await service.accept(_press(data, 2))
        assert accepted.status is IngressStatus.CREATED
        consumer = WorkflowControlConsumer(runtime.settings, factory, owner="control")
        assert await consumer.process_one()
        async with factory() as session:
            step = await session.scalar(select(Step).where(Step.ordinal == 2))
            task = await session.get(Task, task_id)
            assert step is not None and step.status is StepStatus.QUEUED
            assert task is not None and task.status is TaskStatus.RETRYING
            consent = await session.scalar(
                select(Event).where(Event.event_type == "budget.admission_only_consent")
            )
            assert consent is not None
            assert consent.payload["scope"] == "task_family"
    finally:
        await engine.dispose()
