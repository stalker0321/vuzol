"""Same-source recovery is fenced, auditable, and never inherits Start authority."""

import asyncio

import pytest
from sqlalchemy import func, select

from tests.integration.interpretation.test_chat_boundary import chat_runtime
from tests.integration.interpretation.test_chat_work_context_guards import append
from tests.integration.storage.helpers import seed_task_run_step, storage
from tests.integration.workflows._test_runtime_helpers import simple_draft
from vuzol.config import Capability
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.discussion.work_context import freeze_work_context, load_work_context
from vuzol.interpretation.domain import InterpretationResult, TaskOperation, TaskType
from vuzol.interpretation.recovery import RECLASSIFIED_EVENT, replace_unstarted_chat_work
from vuzol.interpretation.work_contract import WORK_CONTRACT_VERSION
from vuzol.storage.attempts import snapshot_task_spec
from vuzol.storage.models import (
    ConversationTurn,
    Effect,
    Event,
    Run,
    Step,
    Task,
    TaskSpecRevision,
    TransactionalOutbox,
)
from vuzol.storage.types import RunStatus, StepStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import build_status_card
from vuzol.workflows.application import Principal
from vuzol.workflows.dispatch import WorkflowDispatcher

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "stale_version",
        "later_turn",
        "started",
        "owner",
        "changed_context",
        "bad_receipt",
        "effect",
    ],
)
def test_only_unstarted_current_work_can_be_reclassified(postgres_dsn, tmp_path, case):
    async def scenario():
        engine, factory = storage(postgres_dsn)
        runtime = chat_runtime(tmp_path)
        old, run_id, step = await seed_task_run_step(factory, step_type="execute_agent")
        raw = "ну окей тогда. давай делать сайт"
        corrected = simple_draft().model_copy(
            update={
                "task_type": TaskType.CODING,
                "operation": TaskOperation.CREATE,
                "goal": raw,
                "project_id": "vuzol",
                "required_capabilities": frozenset(
                    {Capability.REPOSITORY_READ, Capability.CODE_EDIT}
                ),
            }
        )
        result = InterpretationResult(
            draft=corrected,
            profile_id="jev-chat",
            model="typesafe/jev-1.13",
            prompt_version="jev-intake-native-v6",
            duration_ms=1,
        )
        receipt = {
            "effect": "execute_request",
            "abstain": False,
            "profile_id": result.profile_id,
            "model": result.model,
            "selected_choices": {
                "effect": "execute_request",
                "task_type": "coding",
                "operation": "create",
            },
            "work_contract": {"version": WORK_CONTRACT_VERSION, "error": None},
        }
        try:
            async with UnitOfWork(factory) as uow:
                session = uow.session
                discussion_id = await uow.discussions.create_session(
                    project_id="vuzol",
                    chat_id=-100,
                    message_thread_id=10,
                )
                memory = DiscussionMemoryService(uow)
                history_id = await append(
                    memory, discussion_id, "Use the semantic prototype. No deployment."
                )
                prior = await memory.load_context(session_id=discussion_id, chat_context=True)
                source_id = await append(memory, discussion_id, raw)
                task = await session.get(Task, old.id)
                task.project_id = "vuzol"
                task.source_thread_id = 10
                task.ingress_source = "chat_work"
                task.original_text = raw
                task.source_turn_id = source_id
                task.status = TaskStatus.BLOCKED
                task.task_type = "architecture"
                task.task_draft = corrected.model_copy(
                    update={
                        "task_type": TaskType.ARCHITECTURE,
                        "required_capabilities": frozenset({Capability.REPOSITORY_READ}),
                    }
                ).model_dump(mode="json")
                frozen = await freeze_work_context(
                    session,
                    prior=prior,
                    source_turn_id=source_id,
                    project_id="vuzol",
                )
                await snapshot_task_spec(
                    session, task, chat_work_context=frozen.model_dump(mode="json")
                )
                revision, version = task.spec_revision, task.version
                run = await session.get(Run, run_id)
                run.status = RunStatus.BLOCKED
                run.failure_category = "no_compatible_profile"
                step_row = await session.get(Step, step.id)
                step_row.status = StepStatus.BLOCKED
                if case == "started":
                    step_row.attempt_count = 1
                if case == "effect":
                    session.add(
                        Effect(
                            operation_key="test:pending-effect",
                            step_id=step.id,
                            task_id=None,
                            effect_class="publish",
                            target_kind="static",
                            target_reference="test-only",
                            idempotency="unknown",
                            payload_hash="a" * 64,
                            lease_generation=1,
                        )
                    )
                if case == "later_turn":
                    await append(memory, discussion_id, "Now do something else.")
                if case == "changed_context":
                    history = await session.get(ConversationTurn, history_id)
                    history.content = "Different constraints"
            kwargs = dict(
                task_id=old.id,
                expected_version=version + (case == "stale_version"),
                expected_spec_revision=revision,
                principal=Principal(2 if case == "owner" else 1, "cli"),
                result=result,
                receipt=receipt,
            )
            if case == "bad_receipt":
                receipt["selected_choices"]["effect"] = "respond"
            if case != "normal":
                with pytest.raises(ValueError):
                    async with UnitOfWork(factory) as uow:
                        await replace_unstarted_chat_work(uow, runtime, **kwargs)
                async with factory() as session:
                    assert await session.scalar(select(func.count()).select_from(Task)) == 1
                    assert (await session.get(Task, old.id)).status is TaskStatus.BLOCKED
                return
            async with UnitOfWork(factory) as uow:
                fresh_id = await replace_unstarted_chat_work(uow, runtime, **kwargs)
                assert await replace_unstarted_chat_work(uow, runtime, **kwargs) == fresh_id
            assert await WorkflowDispatcher(runtime, factory, owner="recovery").process_one()
            async with factory() as session:
                fresh = await session.get(Task, fresh_id)
                original = await session.get(Task, old.id)
                assert (
                    original.status is TaskStatus.CANCELLED and original.task_type == "architecture"
                )
                assert original.spec_revision == revision
                assert fresh.parent_task_id == old.id and fresh.source_turn_id == source_id
                assert fresh.original_text == raw and fresh.task_type == "coding"
                assert fresh.status is TaskStatus.INTERPRETED
                assert (await session.get(Run, run_id)).status is RunStatus.CANCELLED
                new_run = await session.scalar(select(Run).where(Run.task_id == fresh_id))
                assert new_run.status is RunStatus.CREATED
                assert await session.scalar(select(func.count()).select_from(ConversationTurn)) == 2
                old_spec = await session.scalar(
                    select(TaskSpecRevision).where(
                        TaskSpecRevision.task_id == old.id,
                        TaskSpecRevision.spec_revision == revision,
                    )
                )
                fresh_spec = await session.scalar(
                    select(TaskSpecRevision).where(
                        TaskSpecRevision.task_id == fresh_id,
                        TaskSpecRevision.spec_revision == fresh.spec_revision,
                    )
                )
                assert old_spec.spec["chat_work_context"] == fresh_spec.spec["chat_work_context"]
                assert await load_work_context(session, fresh)
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(Event)
                        .where(
                            Event.event_type == RECLASSIFIED_EVENT,
                        )
                    )
                    == 1
                )
                assert (await build_status_card(session, fresh_id)).buttons == ("start",)
                assert (
                    await session.scalar(
                        select(TransactionalOutbox.id).where(
                            TransactionalOutbox.destination == "telegram",
                            TransactionalOutbox.payload["task_id"].as_string() == str(fresh_id),
                        )
                    )
                    is not None
                )
        finally:
            await engine.dispose()

    asyncio.run(scenario())
