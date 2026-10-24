"""Package-task cards: owner buttons use the card revision and name one criterion."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from telegram import Update
from telegram.error import BadRequest

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
    DeliveryStatus,
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
from vuzol.telegram.projections import TELEGRAM_TEXT_LIMIT, FakeTelegramClient
from vuzol.workflows.acceptance import criterion_choice_token
from vuzol.workflows.application import (
    OwnerDecision,
    Principal,
    TaskCommand,
    apply_task_command,
)
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
    criteria: list[dict[str, object]] | None = None,
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
        if criteria is not None:
            package.exit_criteria = criteria
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
            step_type="acceptance",
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


class _LengthLimitedTelegram(FakeTelegramClient):
    """Transport that refuses a message Telegram itself would refuse."""

    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
        disable_notification: bool = False,
    ) -> int:
        if len(html) > TELEGRAM_TEXT_LIMIT:
            raise BadRequest("message is too long")
        return await super().send_message(
            chat_id=chat_id,
            thread_id=thread_id,
            html=html,
            buttons=buttons,
            task_id=task_id,
            approval_id=approval_id,
            callback_buttons=callback_buttons,
            disable_notification=disable_notification,
        )

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None:
        if len(html) > TELEGRAM_TEXT_LIMIT:
            raise BadRequest("message is too long")
        await super().edit_message(
            chat_id=chat_id,
            message_id=message_id,
            html=html,
            buttons=buttons,
            task_id=task_id,
            approval_id=approval_id,
            callback_buttons=callback_buttons,
        )


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


async def test_package_card_confirms_one_named_criterion(postgres_dsn, tmp_path) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        runtime = telegram_runtime(tmp_path)
        task_id, package_id = await _materialize(factory)
        commit = "b" * 40
        await _block(
            factory,
            task_id,
            package_id,
            category="acceptance_evidence_missing",
            criteria=[
                {
                    "criterion_id": "keeps-saved-work",
                    "statement": _KEEPS,
                    "evidence_kind": "inspection",
                    "observable": True,
                },
                {
                    "criterion_id": "logs-readable",
                    "statement": _LOGS,
                    "evidence_kind": "inspection",
                    "observable": True,
                },
            ],
            result_commit=commit,
        )
        with pytest.raises(ValueError, match="exactly one unproven criterion"):
            async with factory.begin() as session:
                await apply_task_command(
                    session,
                    task_id=task_id,
                    command=TaskCommand.ACCEPT_CRITERION,
                    principal=Principal(42, "cli"),
                    expected_task_version=4,
                    decision=OwnerDecision(reason="owner looked at both"),
                )
        telegram = await _deliver(factory, runtime, task_id)
        html = telegram.sent[0][2]
        assert _KEEPS in html and _LOGS in html
        assert "Каждая кнопка подтверждает только один критерий." in html
        choices = [(label, data) for label, data in _flat(telegram) if data.startswith("v2:c:")]
        assert len(choices) == 2
        assert all(data.split(":")[3] == "40004" for _label, data in choices)
        keeps = criterion_choice_token("keeps-saved-work")
        logs = criterion_choice_token("logs-readable")
        keeps_data = next(data for _label, data in choices if data.endswith(keeps))
        logs_data = next(data for _label, data in choices if data.endswith(logs))
        assert keeps_data != logs_data
        service = TelegramControlService(runtime, factory)
        accepted = await service.accept(_press(keeps_data, 3))
        assert accepted.status is IngressStatus.CREATED
        consumer = WorkflowControlConsumer(runtime.settings, factory, owner="control")
        assert await consumer.process_one()
        async with factory() as session:
            events = (
                await session.scalars(
                    select(Event).where(Event.event_type == "acceptance.owner_criterion")
                )
            ).all()
            assert [event.payload["criterion_id"] for event in events] == ["keeps-saved-work"]
            assert events[0].payload["result_commit"] == commit
        rejected = await service.accept(_press(logs_data, 4))
        assert rejected.status is IngressStatus.REJECTED
        async with factory() as session:
            events = (
                await session.scalars(
                    select(Event).where(Event.event_type == "acceptance.owner_criterion")
                )
            ).all()
            assert [event.payload["criterion_id"] for event in events] == ["keeps-saved-work"]
    finally:
        await engine.dispose()


def _long_statement(index: int) -> str:
    marker = f"LONG-{index}-"
    return marker + ("x" * (800 - len(marker)))


def _rows(keyboard: tuple[tuple[tuple[str, str], ...], ...]) -> list[tuple[str, str]]:
    return [(label, data) for row in keyboard for label, data in row]


async def test_long_criterion_card_is_delivered_page_by_page(postgres_dsn, tmp_path) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        runtime = telegram_runtime(tmp_path)
        task_id, package_id = await _materialize(factory)
        criteria = [
            {
                "criterion_id": f"long-{index}",
                "statement": _long_statement(index),
                "evidence_kind": "inspection",
                "observable": True,
            }
            for index in range(1, 6)
        ]
        await _block(
            factory,
            task_id,
            package_id,
            category="acceptance_evidence_missing",
            criteria=criteria,
            result_commit="d" * 40,
        )
        client = _LengthLimitedTelegram(next_message_id=77)
        await _deliver(factory, runtime, task_id, client)
        assert len(client.sent) == 1
        assert client.sent_silent == [True]
        async with factory() as session:
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"test:owner-card:{task_id}"
                )
            )
            link = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.task_id == task_id)
            )
            assert item is not None and item.status is DeliveryStatus.DELIVERED
            assert link is not None and link.projection_revision == 40004
            assert link.message_id == 77
        pages_html = [client.sent[0][2]]
        keyboards = [client.sent_keyboards[0]]
        service = TelegramControlService(runtime, factory)
        update_id = 20
        nxt: str | None = "pending"
        for _ in range(6):
            flat = _rows(keyboards[-1])
            assert any(label == "Повторить" for label, _data in flat)
            assert any(label == "Изменить" for label, _data in flat)
            nxt = next((data for label, data in flat if label == "→"), None)
            if nxt is None:
                break
            assert nxt.split(":")[3] == "40004"
            turned = await service.accept(_press(nxt, update_id))
            update_id += 1
            assert turned.status is IngressStatus.HANDLED
            assert await _delivery(factory, runtime, client).deliver_one()
            pages_html.append(client.edited[-1][2])
            keyboards.append(client.edited_keyboards[-1])
        assert nxt is None
        assert len(pages_html) >= 2
        assert all(len(html) <= TELEGRAM_TEXT_LIMIT for html in pages_html)
        markers = [f"LONG-{index}-" for index in range(1, 6)]
        covered = [{marker for marker in markers if marker in html} for html in pages_html]
        assert set().union(*covered) == set(markers)
        assert all(page_markers for page_markers in covered)
        assert covered[0].isdisjoint(covered[1])
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.message_id == 77)
            )
            assert link is not None and link.projection_revision == 40004
            before = (
                await session.scalars(
                    select(Event).where(Event.event_type == "acceptance.owner_criterion")
                )
            ).all()
            step = await session.scalar(select(Step).where(Step.ordinal == 2))
            assert before == []
            assert step is not None and step.status is StepStatus.BLOCKED
        tokens = {criterion_choice_token(f"long-{index}"): f"long-{index}" for index in range(1, 6)}
        page_tokens = [
            [data.split(":")[5] for _label, data in _rows(keyboard) if data.startswith("v2:c:")]
            for keyboard in keyboards
        ]
        assert page_tokens[0]
        assert page_tokens[-1]
        chosen_token = next(token for token in page_tokens[-1] if token not in page_tokens[0])
        chosen_id = tokens[chosen_token]
        chosen_data = next(
            data
            for _label, data in _rows(keyboards[-1])
            if data.startswith("v2:c:") and data.endswith(chosen_token)
        )
        accepted = await service.accept(_press(chosen_data, update_id))
        assert accepted.status is IngressStatus.CREATED
        consumer = WorkflowControlConsumer(runtime.settings, factory, owner="control")
        assert await consumer.process_one()
        async with factory() as session:
            events = (
                await session.scalars(
                    select(Event).where(Event.event_type == "acceptance.owner_criterion")
                )
            ).all()
            assert [event.payload["criterion_id"] for event in events] == [chosen_id]
        other = next(data for _label, data in _rows(keyboards[0]) if data.startswith("v2:c:"))
        rejected = await service.accept(_press(other, update_id + 1))
        assert rejected.status is IngressStatus.REJECTED
        async with factory() as session:
            events = (
                await session.scalars(
                    select(Event).where(Event.event_type == "acceptance.owner_criterion")
                )
            ).all()
            assert [event.payload["criterion_id"] for event in events] == [chosen_id]
    finally:
        await engine.dispose()
