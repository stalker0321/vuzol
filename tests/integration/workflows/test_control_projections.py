"""Every accepted user control refreshes the same task card, once."""

import asyncio

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from tests.integration.workflows._test_runtime_helpers import seed_interpreted, simple_draft
from vuzol.storage.models import Task, TransactionalOutbox
from vuzol.workflows.application import Principal, apply_task_command
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.service import materialize_run

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("command", ["start", "pause", "resume", "cancel"])
def test_control_projects_new_revision_but_duplicate_does_not(
    postgres_dsn: str, command: str
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        try:
            task_id, interpretation_id = await seed_interpreted(factory)
            principal = Principal(1, "telegram")
            async with factory.begin() as session:
                task = await session.get(Task, task_id)
                assert task is not None
                task.source_thread_id = 10
                await materialize_run(
                    session,
                    task_id=task_id,
                    workflow=compile_workflow(simple_draft(), interpretation_id=interpretation_id),
                    configuration_revision="a" * 64,
                    policy_revision="b" * 64,
                    prompt_revision="fixture",
                    automatic_start=False,
                )
                if command != "start":
                    await apply_task_command(
                        session, task_id=task_id, command="start", principal=principal
                    )
                if command == "resume":
                    await apply_task_command(
                        session, task_id=task_id, command="pause", principal=principal
                    )
                result = await apply_task_command(
                    session, task_id=task_id, command=command, principal=principal
                )
                assert result.applied
                await session.flush()
                assert (
                    await session.scalar(
                        select(TransactionalOutbox.id).where(
                            TransactionalOutbox.idempotency_key
                            == f"telegram:intake_ack:task:{task_id}:revision:{result.version}"
                        )
                    )
                    is not None
                )
                count = await session.scalar(select(func.count()).select_from(TransactionalOutbox))
                duplicate = await apply_task_command(
                    session, task_id=task_id, command=command, principal=principal
                )
                assert not duplicate.applied
                await session.flush()
                assert (
                    await session.scalar(select(func.count()).select_from(TransactionalOutbox))
                    == count
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())
