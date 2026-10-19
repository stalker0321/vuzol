"""The recorded-shaped inconsistent Jev reply must not become architecture work."""

import asyncio

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select

from tests.integration.interpretation.test_chat_boundary import message
from tests.integration.interpretation.test_chat_intent_context import finish_reply
from tests.integration.interpretation.test_chat_recovery_memory import runtime_with_memory
from tests.integration.storage.helpers import storage
from tests.unit.interpretation.test_work_contract import choices
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.models import Event, Run, Step, Task, TransactionalOutbox
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.dispatch import WorkflowDispatcher

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("kind", ["general", "coding"])
def test_chat_to_work_preserves_intent_or_clarifies_without_dispatch(postgres_dsn, tmp_path, kind):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = runtime_with_memory(tmp_path)
        ingress = TelegramIngressService(runtime, factory)
        bodies = [choices("coding", "modify", "respond"), choices(kind, "create")]
        try:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(lambda _: httpx.Response(200, json=bodies.pop(0)))
            ) as client:
                jev = JevDecisionsInterpreter(
                    credential=SecretStr("test-key"),
                    profile_id="jev-chat",
                    admitted=lambda _: True,
                    client=client,
                )
                pipeline = InterpretationPipeline(
                    runtime, factory, interpreter=jev, owner="contract"
                )
                await ingress.accept_message(
                    message("Обсудим сайт для оценки книг, пока не начинай.")
                )
                assert await pipeline.process_one()
                async with factory() as session:
                    first = (await session.scalars(select(Task))).one()
                    first_id = first.id
                await finish_reply(
                    factory,
                    first_id,
                    "Semantic prototype; no overall score.",
                    client=FakeTelegramClient(),
                )
                await ingress.accept_message(
                    message("ну окей тогда. давай делать сайт").model_copy(
                        update={"message_id": 4102, "update_id": 4102},
                    )
                )
                assert await pipeline.process_one()
                assert not bodies
            async with factory() as session:
                tasks = (await session.scalars(select(Task).order_by(Task.created_at))).all()
                receipts = (
                    await session.scalars(
                        select(Event)
                        .where(
                            Event.event_type == "jev.intake_decided",
                        )
                        .order_by(Event.created_at)
                    )
                ).all()
                assert receipts[-1].payload["selected_choices"]["task_type"] == kind
                if kind == "general":
                    assert len(tasks) == 1
                    assert receipts[-1].payload["work_contract"]["error"] == "work_type_missing"
                    assert (
                        await session.scalar(
                            select(TransactionalOutbox.id).where(
                                TransactionalOutbox.destination == "workflow_dispatch",
                            )
                        )
                        is None
                    )
                else:
                    assert len(tasks) == 2 and tasks[-1].task_type == "coding"
                    assert tasks[-1].ingress_source == "chat_work"
                    assert tasks[-1].original_text == "ну окей тогда. давай делать сайт"
            if kind == "coding":
                assert await WorkflowDispatcher(runtime, factory, owner="contract").process_one()
                async with factory() as session:
                    run = await session.scalar(select(Run).where(Run.task_id == tasks[-1].id))
                    assert run.workflow_type == "coding" and run.status.value == "created"
                    execute = await session.scalar(
                        select(Step).where(
                            Step.run_id == run.id,
                            Step.step_type == "execute_code",
                        )
                    )
                    assert {"code_edit", "project_shell"} <= set(execute.required_capabilities)
                    assert execute.attempt_count == 0  # Explicit Start is still required.
        finally:
            await engine.dispose()

    asyncio.run(scenario())
