"""Discussion snapshot is included before reservations and in every provider role."""

import asyncio
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from tests.integration.interpretation.test_chat_work_context_guards import append
from tests.integration.providers._test_routing_helpers import (
    bundle,
    profile,
    seed_provider_step,
)
from tests.integration.providers.test_handler_request_guards import _request
from tests.integration.storage.helpers import storage
from vuzol.config import ProjectConfig
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.work_context import freeze_work_context, load_work_context
from vuzol.providers.handlers import ProviderStepHandler
from vuzol.providers.health import synchronize_profiles
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import ProviderBudgetReservation, Run, Step, Task
from vuzol.storage.types import StepStatus
from vuzol.storage.unit_of_work import UnitOfWork

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("step_type", ["execute_model", "research_execute", "synthesize"])
@pytest.mark.parametrize("mode", ["normal", "budget", "source"])
def test_frozen_work_context_is_reserved_and_forwarded_before_spend(
    postgres_dsn,
    tmp_path,
    step_type,
    mode,
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        task_id, run_id, step_id = await seed_provider_step(factory, step_type=step_type)
        settings, registries = bundle(
            tmp_path,
            profile("api", context_limit=32_000),
            projects=(
                ProjectConfig(
                    id="proj",
                    display_name="Project",
                    repository_path="proj",
                    default_branch="main",
                    sandbox_profile="unused",
                    allowed_capabilities=frozenset(),
                    enabled=False,
                ),
            ),
        )
        if mode == "budget":
            settings = settings.model_copy(
                update={
                    "limits": settings.limits.model_copy(
                        update={"provider_call_input_tokens": 1000}
                    )
                }
            )
        try:
            async with UnitOfWork(factory) as uow:
                session = uow.session
                discussion_id = await uow.discussions.create_session(
                    project_id="proj", chat_id=-100, message_thread_id=10
                )
                memory = DiscussionMemoryService(uow)
                history_id = await append(
                    memory, discussion_id, "Only local testing. " + "constraints " * 2000
                )
                prior = await memory.load_context(
                    session_id=discussion_id, chat_context=True, delivered_only=True
                )
                source_id = await append(memory, discussion_id, "Begin implementation.")
                task = await session.get(Task, task_id)
                task.project_id = "proj"
                task.original_text = "Begin implementation."
                task.ingress_source = "chat_work"
                task.source_turn_id = source_id
                task.source_thread_id = 10
                snapshot = await freeze_work_context(
                    session, prior=prior, source_turn_id=source_id, project_id="proj"
                )
                await snapshot_task_spec(
                    session, task, chat_work_context=snapshot.model_dump(mode="json")
                )
                run = await session.get(Run, run_id)
                run.selected_route = {"chat_work_spec_revision": task.spec_revision}
                frozen = await load_work_context(session, task)
                await synchronize_profiles(
                    session, registries.profiles.items(), configuration_revision="a" * 64
                )
                if mode == "source":
                    from vuzol.storage.models import ConversationTurn

                    source = await session.get(ConversationTurn, history_id)
                    source.content = "Conditions changed without an agreed spec."
            async with factory.begin() as session:
                token = await claim_routed_step(
                    session,
                    settings=settings,
                    registries=registries,
                    owner="budget-test",
                    lease_seconds=60,
                    candidate_limit=20,
                )
            async with factory() as session:
                reservation = await session.scalar(
                    select(ProviderBudgetReservation).where(
                        ProviderBudgetReservation.step_id == step_id
                    )
                )
                if mode != "normal":
                    assert token is None and reservation is None
                    step = await session.get(Step, step_id)
                    assert step.status is StepStatus.BLOCKED
                    assert step.failure_category == (
                        "chat_work_context_budget_exceeded"
                        if mode == "budget"
                        else "chat_work_context_invalid"
                    )
                    return
                assert token is not None and reservation is not None
                assert reservation.reserved_input_tokens >= (len(snapshot.content) + 3) // 4
            handler = ProviderStepHandler(factory, registries, MagicMock())
            request, *_ = await handler._build_request(
                _request(task_id, run_id, step_id, token, step_type=step_type)
            )
            assert request.original_input == "Begin implementation."
            assert request.context == frozen
        finally:
            await engine.dispose()

    asyncio.run(scenario())
