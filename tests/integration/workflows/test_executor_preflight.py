"""Explicit executor incompatibility blocks before work and never bypasses prerequisites."""

import asyncio

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from tests.integration.telegram.helpers import telegram_runtime
from tests.integration.workflows._test_runtime_helpers import seed_interpreted, simple_draft
from tests.unit.providers.test_pi_adapter import profile
from vuzol.config import Capability, RegistryDocument, RuntimeConfiguration, Settings, build_bundle
from vuzol.config.registries import ProfileRegistry, ProjectRegistry
from vuzol.interpretation.domain import TaskOperation, TaskType
from vuzol.projects.executor_preference import ensure_preference_row
from vuzol.projects.model_matrix import MATRIX_KEY, set_assignment
from vuzol.storage.models import (
    Event,
    ProviderBudgetReservation,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.workflows.controls import retry_blocked_step
from vuzol.workflows.dispatch import WorkflowDispatcher

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    "kind,compatible", [("architecture", False), ("architecture", True), ("coding", True)]
)
def test_assigned_executor_is_checked_before_any_work(postgres_dsn, kind, compatible):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        assigned = profile().model_copy(
            update={
                "id": "assigned",
                "supported_task_types": frozenset({kind if compatible else "coding"}),
                "capabilities": frozenset(
                    {Capability.REPOSITORY_READ, Capability.CODE_EDIT, Capability.PROJECT_SHELL}
                ),
            }
        )
        settings = Settings(environment="test")
        settings = settings.model_copy(
            update={
                "execution": settings.execution.model_copy(
                    update={"default_executor_profile_id": "assigned"}
                ),
            }
        )
        runtime = RuntimeConfiguration(
            settings=settings,
            registries=build_bundle(
                RegistryDocument(profiles=(assigned,)),
                settings,
                validate_profile_credentials=False,
            ),
        )
        draft = simple_draft().model_copy(update={"task_type": TaskType(kind)})
        task_id, interpretation_id = await seed_interpreted(factory, draft)
        try:
            async with factory.begin() as session:
                task = await session.get(Task, task_id)
                task.task_type = kind
                session.add(
                    TransactionalOutbox(
                        destination="workflow_dispatch",
                        operation_type="dispatch_interpretation",
                        linked_entity_type="interpretation",
                        linked_entity_id=interpretation_id,
                        idempotency_key=f"dispatch:{interpretation_id}",
                        payload={},
                    )
                )
            assert await WorkflowDispatcher(runtime, factory, owner="preflight").process_one()
            async with factory() as session:
                run = (await session.scalars(select(Run))).one()
                task = await session.get(Task, task_id)
                steps = (await session.scalars(select(Step).order_by(Step.ordinal))).all()
                assert all(step.attempt_count == 0 for step in steps)
                assert run.status is (RunStatus.CREATED if compatible else RunStatus.BLOCKED)
                assert task.status is (TaskStatus.INTERPRETED if compatible else TaskStatus.BLOCKED)
                assert await session.scalar(select(func.count()).select_from(Worktree)) == 0
                assert (
                    await session.scalar(
                        select(func.count()).select_from(ProviderBudgetReservation)
                    )
                    == 0
                )
                if not compatible:
                    blocked = next(s for s in steps if s.status is StepStatus.BLOCKED)
                    assert blocked.step_type == "ensure_capabilities"
                    assert blocked.failure_category == "executor_route_unavailable"
                    assert (
                        "assigned" in blocked.failure_summary
                        and "architecture" in blocked.failure_summary
                    )
                    assert (
                        await session.scalar(
                            select(Event.id).where(
                                Event.event_type == "workflow.executor_preflight_failed"
                            )
                        )
                        is not None
                    )
                    blocked_id = blocked.id
            if not compatible:
                async with factory.begin() as session:
                    await retry_blocked_step(session, blocked_id, actor_id="1")
                    entry = await session.get(Step, blocked_id)
                    execute = await session.scalar(
                        select(Step).where(Step.step_type == "execute_agent")
                    )
                    assert entry.status is StepStatus.QUEUED
                    assert execute.status is StepStatus.PENDING
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "provider,change",
    [(provider, None) for provider in ("pi", "codex", "grok", "kimi")]
    + [("pi", change) for change in ("disabled", "removed", "revision", "task_type", "capability")],
)
def test_matrix_executor_reaches_dispatch_without_legacy_worker_key(
    postgres_dsn, tmp_path, provider, change
):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = telegram_runtime(tmp_path)
        capabilities = frozenset(
            {Capability.REPOSITORY_READ, Capability.CODE_EDIT, Capability.PROJECT_SHELL}
        )
        chosen = profile().model_copy(
            update={
                "id": "chosen",
                "provider": provider,
                "capabilities": capabilities,
                "supported_task_types": frozenset(
                    {"general" if change == "task_type" else "coding"}
                ),
                "fallback_profile_ids": ("fallback",),
            }
        )
        if change == "capability":
            chosen = chosen.model_copy(
                update={"capabilities": frozenset({Capability.REPOSITORY_READ})}
            )
        fallback = chosen.model_copy(
            update={
                "id": "fallback",
                "runtime_identity": "fixture-fallback",
                "state_directory": tmp_path / "fallback-state",
                "capabilities": capabilities,
                "supported_task_types": frozenset({"coding", "general"}),
                "fallback_profile_ids": (),
            }
        )
        registries = runtime.registries.model_copy(
            update={
                "profiles": ProfileRegistry((chosen, fallback)),
                "projects": ProjectRegistry(
                    (
                        runtime.registries.projects.get("vuzol").model_copy(
                            update={"allowed_capabilities": capabilities}
                        ),
                    ),
                    repository_root=runtime.settings.repository_root,
                ),
            }
        )
        runtime = runtime.model_copy(
            update={
                "registries": registries,
                "settings": runtime.settings.model_copy(
                    update={
                        "interpretation": runtime.settings.interpretation.model_copy(
                            update={"automatic_execution_enabled": True}
                        ),
                    }
                ),
            }
        )
        draft = simple_draft().model_copy(
            update={
                "project_id": "vuzol",
                "task_type": TaskType.CODING,
                "operation": TaskOperation.MODIFY,
            }
        )
        task_id, interpretation_id = await seed_interpreted(factory, draft)
        try:
            async with factory.begin() as session:
                row = await ensure_preference_row(session, "vuzol")
                await set_assignment(
                    session,
                    project_id="vuzol",
                    slot="executor",
                    profile_id="chosen",
                    expected_revision=row.revision,
                    user_id=1,
                    registries=registries,
                )
                assert row.worker_key is None  # the supported generic /model path
                pin = row.model_assignments["executor"]
                task = await session.get(Task, task_id)
                task.project_id = "vuzol"
                task.task_type = "coding"
                session.add(
                    TransactionalOutbox(
                        destination="workflow_dispatch",
                        operation_type="dispatch_interpretation",
                        linked_entity_type="interpretation",
                        linked_entity_id=interpretation_id,
                        idempotency_key=f"dispatch:{interpretation_id}",
                        payload={},
                    )
                )
            if change in {"disabled", "revision", "removed"}:
                changed = chosen.model_copy(
                    update={"enabled": False}
                    if change == "disabled"
                    else {"model": "different-model"}
                )
                runtime = runtime.model_copy(
                    update={
                        "registries": registries.model_copy(
                            update={
                                "profiles": ProfileRegistry(
                                    (fallback,) if change == "removed" else (changed, fallback)
                                )
                            }
                        )
                    }
                )
            assert await WorkflowDispatcher(
                runtime, factory, owner="matrix-preflight"
            ).process_one()
            async with factory() as session:
                run = (await session.scalars(select(Run))).one()
                steps = (await session.scalars(select(Step).order_by(Step.ordinal))).all()
                assert run.selected_route[MATRIX_KEY]["executor"] == pin
                assert all(step.attempt_count == 0 for step in steps)
                assert await session.scalar(select(func.count()).select_from(Worktree)) == 0
                assert (
                    await session.scalar(
                        select(func.count()).select_from(ProviderBudgetReservation)
                    )
                    == 0
                )
                if change is None:
                    assert run.status is RunStatus.RUNNING
                    assert any(step.status is StepStatus.QUEUED for step in steps)
                    assert not any(step.status is StepStatus.BLOCKED for step in steps)
                else:
                    assert run.status is RunStatus.BLOCKED
                    blocked = next(step for step in steps if step.status is StepStatus.BLOCKED)
                    assert blocked.failure_category == "executor_route_unavailable"
                    assert not any(step.status is StepStatus.QUEUED for step in steps)
                    if change in {"task_type", "capability"}:
                        event = (
                            await session.scalars(
                                select(Event).where(
                                    Event.event_type == "workflow.executor_preflight_failed"
                                )
                            )
                        ).one()
                        assert {item["profile_id"] for item in event.payload["evaluations"]} == {
                            "chosen"
                        }
        finally:
            await engine.dispose()

    asyncio.run(scenario())
