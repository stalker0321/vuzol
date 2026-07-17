import asyncio
import uuid

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import storage
from vuzol.storage.models import Step, Task, TelegramMessageLink
from vuzol.storage.types import IdempotencyClass, RunStatus, StepStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import (
    FakeTelegramClient,
    LostTelegramResponse,
    StatusCard,
    apply_status_projection,
    build_status_card,
)

pytestmark = pytest.mark.postgresql


def test_status_card_rebuild_and_revision_guard(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text='<script>alert("x")</script>',
                task_type="coding",
            )
            assert uow.session is not None
            stored = await uow.session.get(Task, task.id)
            assert stored is not None
            stored.task_draft = {
                "normalized_title": "Improve task cards",
                "task_summary": "Show a concise task description in Telegram",
            }
        client = FakeTelegramClient()
        async with factory() as session, session.begin():
            card = await build_status_card(session, task.id)
            assert "<b>Задача №100001</b>" in card.html
            assert "Задача: Show a concise task description in Telegram" in card.html
            assert "&lt;script&gt;" not in card.html
            assert await apply_status_projection(
                session, client, card=card, chat_id=-100, thread_id=10
            )
        async with factory() as session, session.begin():
            stale = StatusCard(task.id, card.revision - 1, "stale")
            assert not await apply_status_projection(
                session, client, card=stale, chat_id=-100, thread_id=10
            )
            newer = StatusCard(task.id, card.revision + 1, "new")
            assert await apply_status_projection(
                session, client, card=newer, chat_id=-100, thread_id=10
            )
        assert len(client.sent) == 1
        assert client.edited == [(-100, 1, "new")]
        await engine.dispose()

    asyncio.run(scenario())


def test_failed_or_lost_send_does_not_create_projection_link(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                original_text="request",
                task_type="coding",
            )
            task_id = task.id
        client = FakeTelegramClient(fail=LostTelegramResponse("unknown outcome"))
        async with factory() as session:
            with pytest.raises(LostTelegramResponse):
                await apply_status_projection(
                    session,
                    client,
                    card=StatusCard(task_id, 1, "status"),
                    chat_id=-100,
                    thread_id=10,
                )
            await session.rollback()
        async with factory() as session:
            assert await session.scalar(select(TelegramMessageLink.id)) is None
        await engine.dispose()

    asyncio.run(scenario())


def test_completed_agent_result_is_rendered_in_project_topic(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Review the architecture",
                task_type="architecture",
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="architecture",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.COMPLETED,
            )
            step = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="execute_agent",
                status=StepStatus.COMPLETED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            assert uow.session is not None
            stored_task = await uow.session.get(Task, task.id)
            stored_step = await uow.session.get(Step, step.id)
            assert stored_task is not None and stored_step is not None
            stored_task.status = TaskStatus.COMPLETED
            stored_step.result = {"text": "Use <ports> and adapters."}

        async with factory() as session:
            card = await build_status_card(session, task.id)
            assert "<b>Результат</b>" in card.html
            assert "Use &lt;ports&gt; and adapters." in card.html
        await engine.dispose()

    asyncio.run(scenario())
