import asyncio
from pathlib import Path

import httpx
import pytest
from pydantic import HttpUrl, SecretStr
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from tests.unit.interpretation.test_jev_live import response
from vuzol.config import (
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
    RegistryDocument,
    RuntimeConfiguration,
    build_bundle,
)
from vuzol.interpretation.jev import JevDecisionsInterpreter
from vuzol.interpretation.service import InterpretationPipeline
from vuzol.storage.models import Event, Run, Task, TransactionalOutbox, UsageRecord
from vuzol.storage.types import TaskStatus
from vuzol.telegram.domain import MessageUpdate
from vuzol.telegram.ingress import TelegramIngressService

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("cancel_during_call", [False, True])
def test_queued_intake_is_consumed_with_receipt_and_usage(
    postgres_dsn: str,
    tmp_path: Path,
    cancel_during_call: bool,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        original = telegram_runtime(tmp_path)
        document = RegistryDocument(
            projects=original.registries.projects.items(),
            topics=original.registries.topics.items(),
            sandboxes=original.registries.sandboxes.items(),
            profiles=(
                ProviderProfileConfig(
                    id="jev-test",
                    provider="jev",
                    model="typesafe/jev-1.13",
                    api_base_url=HttpUrl("https://openrouter.ai/api/alpha/decisions"),
                    launch_mode=LaunchMode.API,
                    credential_required=False,
                    capabilities=frozenset(),
                    concurrency_limit=1,
                    cost_class=CostClass.CHEAP,
                    roles=frozenset({ProviderRole.INTERPRETER}),
                    supported_task_types=frozenset({"general", "coding"}),
                    input_cost_units_per_million=0.042,
                    output_cost_units_per_million=0,
                ),
            ),
        )
        runtime = RuntimeConfiguration(
            settings=original.settings, registries=build_bundle(document, original.settings)
        )
        accepted = await TelegramIngressService(runtime, factory).accept_message(
            MessageUpdate(
                bot_id="main",
                update_id=301,
                chat_id=-100,
                message_thread_id=10,
                message_id=301,
                user_id=42,
                text="Добавь поиск книг",
            )
        )
        assert accepted.task_id is not None

        async def handler(_request: httpx.Request) -> httpx.Response:
            if cancel_during_call:
                async with factory.begin() as session:
                    task = await session.get(Task, accepted.task_id)
                    assert task is not None
                    task.status = TaskStatus.CANCELLED
                    task.version += 1
            return httpx.Response(200, json=response())

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = JevDecisionsInterpreter(
                credential=SecretStr("test-key"),
                profile_id="jev-test",
                admitted=lambda _: True,
                client=client,
            )
            pipeline = InterpretationPipeline(runtime, factory, interpreter=adapter, owner="jev")
            assert await pipeline.process_one()
        async with factory() as session:
            task = await session.get(Task, accepted.task_id)
            assert task is not None
            assert task.status is (
                TaskStatus.CANCELLED if cancel_during_call else TaskStatus.INTERPRETED
            )
            receipts = await session.scalar(
                select(func.count())
                .select_from(Event)
                .where(Event.event_type == "jev.intake_decided")
            )
            assert receipts == (0 if cancel_during_call else 1)
            assert await session.scalar(select(func.count()).select_from(Run)) == 0
            calls = (await session.scalars(select(UsageRecord))).all()
            assert len(calls) == 1
            assert calls[0].input_tokens == 120
            if not cancel_during_call:
                dispatch = await session.scalar(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.destination == "workflow_dispatch"
                    )
                )
                assert dispatch is not None
        await engine.dispose()

    asyncio.run(scenario())
