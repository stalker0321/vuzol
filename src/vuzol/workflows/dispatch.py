"""Fenced interpretation disposition and workflow materialization."""

import hashlib
import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.interpretation.domain import TaskAction, TaskDraft
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item, dead_letter_outbox_item
from vuzol.storage.models import (
    Interpretation,
    ProjectProvisioning,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import ProjectProvisioningStatus, RunStatus, StepStatus, TaskStatus
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.controls import cancel_task, pause_task, resume_task
from vuzol.workflows.service import materialize_run
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

POLICY_REVISION = hashlib.sha256(b"step-06-workflow-policy-v1").hexdigest()
WORKFLOW_ALIASES = {
    "coding_task": "coding.v1",
    "simple_model_task": "simple_model.v1",
    "research_task": "research.v1",
    "infrastructure_task": "infrastructure.v1",
}


class WorkflowDispatchError(RuntimeError):
    pass


class WorkflowDispatcher:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
    ) -> None:
        self._runtime = runtime
        self._factory = session_factory
        self._owner = owner

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._runtime.settings.workflow.lease_seconds,
                allowed_destinations=frozenset({"workflow_dispatch"}),
            )
        if token is None:
            return False
        try:
            async with self._factory.begin() as session:
                await self._dispatch(session, token)
                await complete_outbox_item(session, token)
        except (ValueError, WorkflowDispatchError):
            async with self._factory.begin() as session:
                await dead_letter_outbox_item(
                    session, token, error_category="invalid_workflow_dispatch"
                )
        return True

    async def _dispatch(self, session: AsyncSession, token: OutboxLeaseToken) -> None:
        item = await session.get(TransactionalOutbox, token.item_id)
        if item is None or item.linked_entity_type != "interpretation":
            raise WorkflowDispatchError("interpretation dispatch item is invalid")
        interpretation = await session.get(Interpretation, item.linked_entity_id)
        if interpretation is None:
            raise WorkflowDispatchError("interpretation not found")
        task = await session.scalar(
            select(Task).where(Task.id == interpretation.task_id).with_for_update()
        )
        if task is None:
            raise WorkflowDispatchError("carrier task not found")
        draft = TaskDraft.model_validate(interpretation.task_draft)
        if draft.needs_clarification:
            raise WorkflowDispatchError("clarification is unresolved")

        if draft.action is TaskAction.CREATE_PROJECT:
            await self._create_project_request(session, task, draft)
            return

        if draft.action in {
            TaskAction.CREATE_TASK,
            TaskAction.ANSWER_QUESTION,
            TaskAction.GENERAL_CONVERSATION,
        }:
            configured = await self._configured_workflow(session, task, draft)
            workflow = compile_workflow(
                draft,
                interpretation_id=interpretation.id,
                configured_workflow=configured,
            )
            run = await materialize_run(
                session,
                task_id=task.id,
                workflow=workflow,
                configuration_revision=self._runtime.registries.revision,
                policy_revision=POLICY_REVISION,
                prompt_revision=interpretation.prompt_version,
                automatic_start=self._runtime.settings.interpretation.automatic_execution_enabled,
            )
            await self._enqueue_task_projection(session, task, run)
            return
        if draft.action is TaskAction.CONTINUE_TASK:
            await self._continue_task(session, task, draft, interpretation.id)
            return
        if draft.action in {
            TaskAction.PAUSE_TASK,
            TaskAction.RESUME_TASK,
            TaskAction.CANCEL_TASK,
        }:
            if draft.referenced_task_id is None:
                raise WorkflowDispatchError("control target is ambiguous")
            actor_id = str(task.user_id)
            operation = {
                TaskAction.PAUSE_TASK: pause_task,
                TaskAction.RESUME_TASK: resume_task,
                TaskAction.CANCEL_TASK: cancel_task,
            }[draft.action]
            await operation(session, draft.referenced_task_id, actor_id=actor_id)
            await self._complete_carrier(session, task, draft.referenced_task_id)
            return
        if draft.action in {TaskAction.APPROVE_STEP, TaskAction.REJECT_STEP}:
            if task.status is TaskStatus.INTERPRETED:
                await transition_task(
                    session,
                    task,
                    TaskStatus.AWAITING_USER,
                    actor_type="workflow_manager",
                    payload={"reason": "explicit_approval_control_required"},
                )
            return
        raise WorkflowDispatchError(f"unsupported action: {draft.action.value}")

    async def _create_project_request(
        self,
        session: AsyncSession,
        task: Task,
        draft: TaskDraft,
    ) -> None:
        if draft.new_project_id is None or draft.new_project_name is None:
            raise WorkflowDispatchError("project identity is missing")
        if task.source_thread_id is None:
            raise WorkflowDispatchError("project provisioning requires a forum topic")
        lock_key = int.from_bytes(
            hashlib.sha256(f"project:{draft.new_project_id}".encode()).digest()[:8],
            signed=True,
        )
        await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
        existing = await session.scalar(
            select(ProjectProvisioning).where(ProjectProvisioning.task_id == task.id)
        )
        if existing is not None:
            return
        conflict = await session.scalar(
            select(ProjectProvisioning.id).where(
                ProjectProvisioning.project_id == draft.new_project_id
            )
        )
        if conflict is not None:
            raise WorkflowDispatchError("project identity is already being provisioned")
        provisioning = ProjectProvisioning(
            task_id=task.id,
            requested_by_user_id=task.user_id,
            chat_id=task.source_chat_id,
            source_thread_id=task.source_thread_id,
            project_id=draft.new_project_id,
            display_name=draft.new_project_name,
            description=draft.goal,
            repository_path=draft.new_project_id,
            status=ProjectProvisioningStatus.PENDING,
        )
        session.add(provisioning)
        await session.flush()
        task.project_id = provisioning.project_id
        await transition_task(
            session,
            task,
            TaskStatus.EXECUTING,
            actor_type="project_provisioning",
        )
        session.add(
            TransactionalOutbox(
                destination="project_provisioning",
                operation_type="create_project",
                linked_entity_type="project_provisioning",
                linked_entity_id=provisioning.id,
                idempotency_key=f"project:provision:{provisioning.id}",
                payload={"project_id": provisioning.project_id},
            )
        )
        intake = await session.scalar(
            select(TelegramIntakeMessage)
            .where(TelegramIntakeMessage.task_id == task.id)
            .order_by(TelegramIntakeMessage.created_at.desc())
            .limit(1)
        )
        if intake is not None:
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="telegram_intake",
                    linked_entity_id=intake.id,
                    idempotency_key=(f"telegram:project:{provisioning.id}:status:{task.version}"),
                    payload={"role": "intake_ack", "task_id": str(task.id)},
                )
            )

    async def _configured_workflow(
        self, session: AsyncSession, task: Task, draft: TaskDraft
    ) -> str | None:
        if draft.action in {TaskAction.ANSWER_QUESTION, TaskAction.GENERAL_CONVERSATION}:
            return "simple_model.v1"
        topic = await session.scalar(
            select(TopicMapping).where(
                TopicMapping.chat_id == task.source_chat_id,
                TopicMapping.message_thread_id == task.source_thread_id,
            )
        )
        if topic is None:
            return None
        return WORKFLOW_ALIASES.get(topic.default_workflow, topic.default_workflow)

    async def _enqueue_task_projection(self, session: AsyncSession, task: Task, run: Run) -> None:
        intake = await session.scalar(
            select(TelegramIntakeMessage)
            .where(TelegramIntakeMessage.task_id == task.id)
            .order_by(TelegramIntakeMessage.created_at.desc())
            .limit(1)
        )
        if intake is None:
            return
        session.add(
            TransactionalOutbox(
                destination="telegram",
                operation_type="send_message",
                linked_entity_type="telegram_intake",
                linked_entity_id=intake.id,
                idempotency_key=f"telegram:workflow:{run.id}:revision:{task.version}",
                payload={
                    "chat_id": intake.chat_id,
                    "message_thread_id": intake.message_thread_id,
                    "role": "intake_ack",
                    "task_id": str(task.id),
                },
            )
        )

    async def _continue_task(
        self,
        session: AsyncSession,
        carrier: Task,
        draft: TaskDraft,
        interpretation_id: uuid.UUID,
    ) -> None:
        if draft.referenced_task_id is None:
            raise WorkflowDispatchError("continuation target is missing")
        target = await session.scalar(
            select(Task).where(Task.id == draft.referenced_task_id).with_for_update()
        )
        if target is None:
            raise WorkflowDispatchError("continuation target not found")
        run = await session.scalar(
            select(Run)
            .where(Run.task_id == target.id)
            .order_by(Run.created_at.desc())
            .with_for_update()
        )
        if run is None or run.status is not RunStatus.AWAITING_USER:
            raise WorkflowDispatchError("target is not awaiting user input")
        step = await session.scalar(
            select(Step)
            .where(Step.run_id == run.id, Step.status == StepStatus.AWAITING_USER)
            .order_by(Step.ordinal)
            .with_for_update()
        )
        if step is None:
            raise WorkflowDispatchError("awaiting-user step not found")
        step.payload = {
            **step.payload,
            "continuation_interpretation_id": str(interpretation_id),
        }
        await transition_step(session, step, StepStatus.QUEUED, actor_type="workflow_manager")
        await transition_run(session, run, RunStatus.RUNNING, actor_type="workflow_manager")
        await transition_task(session, target, TaskStatus.EXECUTING, actor_type="workflow_manager")
        await self._complete_carrier(session, carrier, target.id)

    async def _complete_carrier(
        self, session: AsyncSession, carrier: Task, parent_task_id: uuid.UUID
    ) -> None:
        carrier.parent_task_id = parent_task_id
        await transition_task(
            session,
            carrier,
            TaskStatus.COMPLETED,
            actor_type="workflow_manager",
            payload={
                "disposition": "control_or_continuation",
                "target_task_id": str(parent_task_id),
            },
        )
        carrier.completed_at = func.now()
