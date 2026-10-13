"""Actual chat→fresh card→callback→same-task retry, real TEST PostgreSQL."""

import json
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from telegram import Update

from tests.integration.interpretation.test_chat_boundary import chat_runtime, message
from tests.integration.storage.helpers import seed_task_run_step, storage
from tests.unit.interpretation.test_jev_live import response
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import (
    Event,
    Run,
    Step,
    Task,
    TelegramControlAction,
    TelegramMessageLink,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.types import (
    ControlActionStatus,
    IdempotencyClass,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.telegram.adapter import _control_markup, control_update
from vuzol.telegram.controls import TelegramControlService
from vuzol.telegram.delivery import TelegramDeliveryService, prepare_delivery
from vuzol.telegram.domain import IngressStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import (
    FakeTelegramClient,
    build_status_card,
    enqueue_task_control_snapshot,
)
from vuzol.workflows.application import Principal, apply_task_command
from vuzol.workflows.controls import WorkflowControlConsumer
from vuzol.workflows.recovery_policy import RecoveryPolicy
from vuzol.workflows.service import _recovery_deadline_exceeded

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def callback(data, message_id, update_id, *, user=42, thread=10):
    converted = control_update(
        Update.de_json(
            {
                "update_id": update_id,
                "callback_query": {
                    "id": f"callback-{update_id}",
                    "from": {"id": user, "is_bot": False, "first_name": "Owner"},
                    "chat_instance": "test",
                    "data": data,
                    "message": {
                        "message_id": message_id,
                        "message_thread_id": thread,
                        "date": 0,
                        "chat": {"id": -100, "type": "supergroup"},
                    },
                },
            },
            None,
        ),
        "main",
    )
    assert converted is not None
    return converted


async def stopped(factory):
    record, run_id, step_record = await seed_task_run_step(factory, step_type="execute_code")
    async with factory.begin() as session:
        task = await session.get(Task, record.id)
        task.user_id, task.source_chat_id, task.source_thread_id = 42, -100, 10
        task.project_id = "vuzol"
        task.original_text = "Сделай личный тест; если данных нет, не придумывай числа."
        task.task_draft = {"goal": task.original_text, "constraints": ["без регистрации"]}
        task.status = TaskStatus.QUOTA_EXHAUSTED
        revision = await snapshot_task_spec(session, task)
        run = await session.get(Run, run_id)
        run.status = RunStatus.BLOCKED
        started = datetime.now(UTC) - timedelta(hours=4)
        run.started_at = started
        step = await session.get(Step, step_record.id)
        step.status = StepStatus.BLOCKED
        step.failure_category = "budget_exhausted"
        step.failure_summary = "legacy wrong provider-quota label"
        session.add(
            UsageRecord(
                provider="old",
                profile_id="retired",
                model="old",
                task_id=task.id,
                run_id=run_id,
                step_id=step.id,
                input_tokens=9000,
                output_tokens=25000,
                duration_ms=1,
                outcome="provider_unavailable",
            )
        )
        task_id, step_id = task.id, step.id
    return task_id, run_id, step_id, revision, started


@pytest.mark.parametrize("effect", ["execute_request", "control_request"])
async def test_natural_continuation_posts_working_controls_without_start(
    effect, postgres_dsn, tmp_path
):
    engine, factory = storage(postgres_dsn)
    try:
        runtime = chat_runtime(tmp_path)
        task_id, run_id, step_id, revision, started = await stopped(factory)
        async with factory.begin() as session:
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=9,
                    task_id=task_id,
                    message_role="task_status",
                    projection_revision=1,
                )
            )
        calls = []

        def classify(request):
            calls.append(json.loads(request.content))
            return httpx.Response(200, json=response(effect, "continue", str(task_id)))

        async with httpx.AsyncClient(transport=httpx.MockTransport(classify)) as client:
            interpreter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-chat",
                admitted=lambda _: True,
                client=client,
            )
            await TelegramIngressService(runtime, factory).accept_message(message("продолжи"))
            pipeline = InterpretationPipeline(
                runtime, factory, owner="test", interpreter=interpreter
            )
            assert await pipeline.process_one()
        assert len(calls) == 1
        async with factory() as session:
            task = await session.get(Task, task_id)
            assert task.status is TaskStatus.QUOTA_EXHAUSTED and task.budget_epoch == 0
            assert task.spec_revision == revision
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.payload["role"].as_string() == "task_control_snapshot"
                )
            )
            assert item is not None
            prepared = await prepare_delivery(session, item)
            assert (
                "внутренний бюджет" in prepared.html and "спецификация не изменена" in prepared.html
            )
            assert prepared.callback_buttons[0][0][1].startswith("v2:retry:")
        telegram = FakeTelegramClient(next_message_id=77)
        delivery = TelegramDeliveryService(
            factory,
            telegram,
            owner="delivery",
            lease_seconds=30,
            max_attempts=2,
            retry_min_seconds=1,
            retry_max_seconds=2,
            topics=runtime.registries.topics,
            projects=runtime.registries.projects,
        )
        for _ in range(8):
            if not await delivery.deliver_one():
                break
        async with factory() as session:
            link = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.message_role == "task_status")
            )
            assert link is not None and link.task_id == task_id
            assert link.message_id == 77
            old = await session.scalar(
                select(TelegramMessageLink).where(TelegramMessageLink.message_id == 9)
            )
            assert old.message_role == "task_status_archived"
        data = telegram.sent_keyboards[0][0][0][1]
        service = TelegramControlService(runtime, factory)
        first = callback(data, link.message_id, 1201)
        assert (await service.accept(first)).status is IngressStatus.CREATED
        assert (await service.accept(first)).status is IngressStatus.DUPLICATE
        # A second physical tap queued before the first is consumed must not
        # grant another attempt or budget epoch at mutation time.
        assert (
            await service.accept(callback(data, link.message_id, 1202))
        ).status is IngressStatus.CREATED
        consumer = WorkflowControlConsumer(runtime.settings, factory, owner="control")
        assert await consumer.process_one()
        assert await consumer.process_one()
        assert await delivery.deliver_one()
        assert telegram.edited and telegram.edited[0][1] == 77
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            step = await session.get(Step, step_id)
            assert step.status is StepStatus.QUEUED and run.status is RunStatus.RUNNING
            assert task.budget_epoch == 1 and task.spec_revision == revision
            assert run.started_at == started
            assert not _recovery_deadline_exceeded(run, step, RecoveryPolicy())
            actions = (
                await session.scalars(
                    select(TelegramControlAction).order_by(TelegramControlAction.created_at)
                )
            ).all()
            assert [a.status for a in actions] == [
                ControlActionStatus.PROCESSED,
                ControlActionStatus.REJECTED,
            ]
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(Event)
                    .where(Event.event_type == "task.budget_epoch_started")
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(UsageRecord)
                    .where(UsageRecord.profile_id == "retired")
                )
                == 1
            )
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
        stale = await service.accept(callback(data, link.message_id, 1203))
        assert stale.status is IngressStatus.REJECTED and stale.reason == "task_retry_stale"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("bound", [True, False])
async def test_retry_bound_repair_not_unrelated_second_block(postgres_dsn, bound):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id, step_id, revision, started = await stopped(factory)
        parent_id = uuid.uuid4()
        async with factory.begin() as session:
            session.add(
                Step(
                    id=parent_id,
                    run_id=run_id,
                    ordinal=0,
                    step_type="validate",
                    status=StepStatus.BLOCKED,
                    unknown_effects=False,
                    timeout_seconds=60,
                    idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                    failure_category="validation_gate_failed",
                    payload={"repair_scheduled_step_id": str(step_id)} if bound else {},
                )
            )
            child = await session.get(Step, step_id)
            child.payload = {"repair_for_step_id": str(parent_id)}
            child.dependency_metadata = {"template_key": "repair_code", "repair_for_ordinal": 0}
        async with factory.begin() as session:
            if bound:
                result = await apply_task_command(
                    session,
                    task_id=task_id,
                    command="retry",
                    principal=Principal(42, "cli"),
                    expected_task_version=1,
                    step_id=step_id,
                )
                assert result.applied
            else:
                with pytest.raises(ValueError, match="unambiguous"):
                    await apply_task_command(
                        session,
                        task_id=task_id,
                        command="retry",
                        principal=Principal(42, "cli"),
                        expected_task_version=1,
                        step_id=step_id,
                    )
        async with factory() as session:
            task = await session.get(Task, task_id)
            run = await session.get(Run, run_id)
            parent = await session.get(Step, parent_id)
            child = await session.get(Step, step_id)
            assert task.spec_revision == revision and run.started_at == started
            assert task.budget_epoch == (1 if bound else 0)
            assert parent.status is StepStatus.BLOCKED
            assert child.status is (StepStatus.QUEUED if bound else StepStatus.BLOCKED)
    finally:
        await engine.dispose()


async def test_controls_reject_foreign_scope_and_unknown_effects(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    try:
        runtime = chat_runtime(tmp_path)
        task_id, run_id, step_id, revision, _ = await stopped(factory)
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            card = await build_status_card(session, task_id)
            data = card.callback_buttons[0][0][1]
            session.add(
                TelegramMessageLink(
                    chat_id=-100,
                    message_thread_id=10,
                    message_id=77,
                    task_id=task_id,
                    message_role="task_status",
                    projection_revision=task.version,
                )
            )
        service = TelegramControlService(runtime, factory)
        result = await service.accept(callback(data, 77, 1301, thread=11))
        assert result.status is IngressStatus.REJECTED
        async with factory.begin() as session:
            step = await session.get(Step, step_id)
            step.unknown_effects = True
        async with factory() as session:
            card = await build_status_card(session, task_id)
            assert not card.callback_buttons and "небезопасен" in card.html
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="safely retryable"):
                await apply_task_command(
                    session,
                    task_id=task_id,
                    command="retry",
                    principal=Principal(42, "cli"),
                    expected_task_version=1,
                    step_id=step_id,
                )
            task = await session.get(Task, task_id)
            assert task.budget_epoch == 0 and task.spec_revision == revision
        async with factory.begin() as session:
            run = await session.get(Run, run_id)
            run.selected_route = {"recovery_epoch_started_at": "bad timestamp"}
            step = await session.get(Step, step_id)
            assert _recovery_deadline_exceeded(run, step, RecoveryPolicy())
    finally:
        await engine.dispose()


async def test_versioned_pause_survives_real_markup_without_duplicate_button(
    postgres_dsn, tmp_path
):
    engine, factory = storage(postgres_dsn)
    try:
        task_id, run_id, _, _, _ = await stopped(factory)
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            task.status = TaskStatus.EXECUTING
            run = await session.get(Run, run_id)
            run.status = RunStatus.RUNNING
            await enqueue_task_control_snapshot(
                session, task=task, chat_id=-100, thread_id=10, user_id=42, key="snapshot"
            )
            card = await build_status_card(session, task_id)
            markup = _control_markup(
                card.buttons,
                task_id=task_id,
                approval_id=None,
                callback_buttons=card.callback_buttons,
            )
            assert markup is not None and len(markup.inline_keyboard) == 2
            assert all(
                b.callback_data.startswith("v2:task:")
                for row in markup.inline_keyboard
                for b in row
            )
    finally:
        await engine.dispose()
