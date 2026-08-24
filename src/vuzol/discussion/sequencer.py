"""Deterministic approved-plan to canonical-Task sequencing."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, Settings
from vuzol.discussion.domain import (
    DomainError,
    PackageControlAction,
    WorkPackageEvent,
    control_transition_target,
    require_generation,
)
from vuzol.interpretation.domain import (
    TASK_DRAFT_SCHEMA_VERSION,
    SuggestedComplexity,
    TaskAction,
    TaskDraft,
    TaskOperation,
    TaskType,
)
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
)
from vuzol.storage.models import (
    Interpretation,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    Run,
    Step,
    Task,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    EstimatedComplexity,
    PlanRevisionState,
    StepStatus,
    TaskStatus,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork

MATERIALIZER_PROFILE = "work_package_materializer"
MATERIALIZER_MODEL = "deterministic"
MATERIALIZER_PROMPT_VERSION = "work-package-materializer-v1"


@dataclass(frozen=True, slots=True)
class SequenceResult:
    package_id: uuid.UUID
    status_generation: int
    task_id: uuid.UUID | None
    ordinal: int | None
    completed: bool = False


def _same_plan_item(left: PlanRevisionItem, right: PlanRevisionItem) -> bool:
    fields = (
        "summary",
        "goal",
        "expected_outcome",
        "completion_criteria",
        "allowed_scope",
        "out_of_scope",
        "dependencies",
        "trusted_checks",
        "suggested_risk",
        "needs_approval",
        "estimated_complexity",
    )
    return all(getattr(left, field) == getattr(right, field) for field in fields)


class WorkPackageSequencer:
    """Transaction-owned, one-ahead sequencer; it never invokes a model."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def start(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> SequenceResult:
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        control_transition_target(package.status, PackageControlAction.START)
        revision = await self._fenced_approved_revision(
            package_id, revision_number=revision_number, h8=h8
        )
        if package.approved_revision_id != revision.id or package.head_revision_id != revision.id:
            raise DomainError("approval_binding_mismatch")

        previous = package.status
        package.status = WorkPackageStatus.RUNNING
        package.running_revision_id = revision.id
        package.cursor_ordinal = await self._first_unfinished_ordinal(package.id, revision)
        package.pause_reason = None
        package.last_failure_task_id = None
        package.version += 1
        package.start_generation = package.version
        await self._uow.events.append(
            entity_type="work_package",
            entity_id=package.id,
            event_type=WorkPackageEvent.PACKAGE_STARTED.value,
            actor_type="user",
            previous_state=previous.value,
            new_state=package.status.value,
            payload={
                "revision_id": str(revision.id),
                "start_generation": package.start_generation,
                "started_by_user_id": user_id,
            },
        )
        return await self._materialize_current(package, revision)

    async def _first_unfinished_ordinal(self, package_id: uuid.UUID, revision: PlanRevision) -> int:
        """Carry an unchanged, completed prefix across plan revisions."""

        assert self._uow.session is not None
        current_items = tuple(
            (
                await self._uow.session.scalars(
                    select(PlanRevisionItem)
                    .where(
                        PlanRevisionItem.work_package_id == package_id,
                        PlanRevisionItem.plan_revision_id == revision.id,
                    )
                    .order_by(PlanRevisionItem.ordinal)
                )
            ).all()
        )
        for item in current_items:
            completed_predecessors = tuple(
                (
                    await self._uow.session.scalars(
                        select(PlanRevisionItem)
                        .join(
                            MaterializationLink,
                            MaterializationLink.plan_revision_item_id == PlanRevisionItem.id,
                        )
                        .join(Task, Task.id == MaterializationLink.task_id)
                        .where(
                            MaterializationLink.work_package_id == package_id,
                            MaterializationLink.plan_revision_id != revision.id,
                            MaterializationLink.work_item_draft_id == item.item_id,
                            Task.status == TaskStatus.COMPLETED,
                        )
                    )
                ).all()
            )
            if not any(_same_plan_item(item, previous) for previous in completed_predecessors):
                return item.ordinal
        return len(current_items) + 1

    async def observe_terminal(self, *, task_id: uuid.UUID) -> SequenceResult | None:
        """Consume terminal evidence once; duplicate observations are harmless."""

        assert self._uow.session is not None
        link = await self._uow.session.scalar(
            select(MaterializationLink)
            .where(MaterializationLink.task_id == task_id)
            .with_for_update()
        )
        if link is None:
            return None
        package = await self._uow.work_packages.get_package(link.work_package_id, for_update=True)
        task = await self._uow.session.get(Task, task_id)
        if task is None:
            raise DomainError("materialized_task_missing")
        if task.status not in {
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.BLOCKED,
            TaskStatus.QUOTA_EXHAUSTED,
        }:
            raise DomainError("task_not_terminal")
        if (
            package.status is not WorkPackageStatus.RUNNING
            or package.running_revision_id != link.plan_revision_id
            or package.cursor_ordinal != link.ordinal
        ):
            return SequenceResult(
                package.id,
                package.version,
                task.id,
                link.ordinal,
                package.status is WorkPackageStatus.COMPLETED,
            )
        revision = await self._uow.work_packages.get_revision(link.plan_revision_id)
        if task.status in {
            TaskStatus.FAILED,
            TaskStatus.BLOCKED,
            TaskStatus.QUOTA_EXHAUSTED,
        }:
            package.status = WorkPackageStatus.PAUSED
            from vuzol.storage.types import WorkPackagePauseReason

            package.pause_reason = (
                WorkPackagePauseReason.ITEM_BLOCKED
                if task.status in {TaskStatus.BLOCKED, TaskStatus.QUOTA_EXHAUSTED}
                else WorkPackagePauseReason.ITEM_FAILED
            )
            package.last_failure_task_id = task.id
            package.version += 1
            await self._uow.events.append(
                entity_type="work_package",
                entity_id=package.id,
                event_type=WorkPackageEvent.PACKAGE_PAUSED.value,
                actor_type="system",
                previous_state=WorkPackageStatus.RUNNING.value,
                new_state=WorkPackageStatus.PAUSED.value,
                payload={
                    "ordinal": link.ordinal,
                    "failure_task_id": str(task.id),
                    "reason": package.pause_reason.value,
                },
            )
            await self._projection(package.id, package.version, "terminal_pause")
            await self._task_projection(task, package.version, "terminal_pause")
            return SequenceResult(package.id, package.version, task.id, link.ordinal)

        package.cursor_ordinal = link.ordinal + 1
        package.version += 1
        return await self._materialize_current(package, revision)

    async def materialize_running(self, *, package_id: uuid.UUID) -> SequenceResult:
        """Materialize the fenced cursor after an explicit skip/recovery transition."""

        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        if package.status is not WorkPackageStatus.RUNNING:
            raise DomainError("invalid_transition")
        if package.running_revision_id is None:
            raise DomainError("running_revision_missing")
        revision = await self._uow.work_packages.get_revision(package.running_revision_id)
        if revision.id != package.head_revision_id:
            raise DomainError("stale_revision")
        return await self._materialize_current(package, revision)

    async def retry_failed_item(
        self,
        *,
        package_id: uuid.UUID,
        revision_number: int,
        h8: str,
        expected_status_generation: int,
        user_id: int,
    ) -> SequenceResult:
        """Materialize a fresh Task while retaining the failed attempt as evidence."""

        from vuzol.storage.types import WorkPackagePauseReason
        from vuzol.workflows.retry_policy import failed_item_is_rematerializable

        assert self._uow.session is not None
        package = await self._uow.work_packages.get_package(package_id, for_update=True)
        require_generation(package.version, expected_status_generation)
        revision = await self._fenced_approved_revision(
            package_id, revision_number=revision_number, h8=h8
        )
        if (
            package.status is not WorkPackageStatus.PAUSED
            or package.pause_reason is not WorkPackagePauseReason.ITEM_FAILED
            or package.cursor_ordinal is None
            or package.last_failure_task_id is None
            or package.running_revision_id != revision.id
        ):
            raise DomainError("item_not_safely_retryable")
        failed_step = await self._uow.session.scalar(
            select(Step)
            .join(Run, Run.id == Step.run_id)
            .where(
                Run.task_id == package.last_failure_task_id,
                Step.status == StepStatus.FAILED,
            )
            .order_by(Step.ordinal.desc())
            .limit(1)
        )
        if failed_step is None or not failed_item_is_rematerializable(failed_step):
            raise DomainError("item_not_safely_retryable")
        link = await self._uow.session.scalar(
            select(MaterializationLink)
            .where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == package.cursor_ordinal,
                MaterializationLink.task_id == package.last_failure_task_id,
            )
            .with_for_update()
        )
        if link is None:
            raise DomainError("failure_context_missing")
        item = await self._uow.session.scalar(
            select(PlanRevisionItem).where(PlanRevisionItem.id == link.plan_revision_item_id)
        )
        discussion = await self._uow.session.get(ProjectDiscussionSession, package.session_id)
        if item is None or discussion is None:
            raise DomainError("failure_context_missing")
        draft = _task_draft(package.project_id, item)
        original = _original_text(item)
        task_record = await self._uow.tasks.create(
            user_id=user_id,
            chat_id=discussion.chat_id,
            thread_id=discussion.message_thread_id,
            project_id=package.project_id,
            original_text=original,
            task_type=draft.task_type.value,
            task_draft=draft.model_dump(mode="json"),
        )
        task = await self._uow.session.get(Task, task_record.id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.INTERPRETED
        task.interpreter_profile = MATERIALIZER_PROFILE
        task.prompt_version = MATERIALIZER_PROMPT_VERSION
        task.draft_schema_version = TASK_DRAFT_SCHEMA_VERSION
        task.version += 1
        interpretation = Interpretation(
            task_id=task.id,
            original_input_hash=hashlib.sha256(original.encode()).hexdigest(),
            transcript=None,
            task_draft=draft.model_dump(mode="json"),
            profile_id=MATERIALIZER_PROFILE,
            model=MATERIALIZER_MODEL,
            prompt_version=MATERIALIZER_PROMPT_VERSION,
            schema_version=TASK_DRAFT_SCHEMA_VERSION,
        )
        self._uow.session.add(interpretation)
        await self._uow.session.flush()
        previous_task_id = link.task_id
        link.task_id = task.id
        link.materialized_at = func.now()
        package.status = WorkPackageStatus.RUNNING
        package.pause_reason = None
        package.last_failure_task_id = None
        package.version += 1
        await self._uow.outbox.enqueue(
            destination="workflow_dispatch",
            operation_type="dispatch_interpretation",
            entity_type="interpretation",
            entity_id=interpretation.id,
            idempotency_key=f"workflow:dispatch:{interpretation.id}",
            payload={
                "task_id": str(task.id),
                "work_package_id": str(package.id),
                "ordinal": link.ordinal,
            },
        )
        await self._uow.events.append(
            entity_type="work_package",
            entity_id=package.id,
            event_type=WorkPackageEvent.PACKAGE_RETRIED.value,
            actor_type="user",
            previous_state=WorkPackageStatus.PAUSED.value,
            new_state=WorkPackageStatus.RUNNING.value,
            payload={
                "ordinal": link.ordinal,
                "previous_task_id": str(previous_task_id),
                "task_id": str(task.id),
                "requested_by_user_id": user_id,
            },
        )
        await self._projection(package.id, package.version, "failed_item_retry")
        return SequenceResult(package.id, package.version, task.id, link.ordinal)

    async def _materialize_current(self, package: object, revision: PlanRevision) -> SequenceResult:
        from vuzol.storage.models import WorkPackage

        assert isinstance(package, WorkPackage)
        assert self._uow.session is not None
        ordinal = package.cursor_ordinal
        if ordinal is None:
            raise DomainError("cursor_missing")
        existing = await self._uow.session.scalar(
            select(MaterializationLink).where(
                MaterializationLink.work_package_id == package.id,
                MaterializationLink.plan_revision_id == revision.id,
                MaterializationLink.ordinal == ordinal,
            )
        )
        if existing is not None:
            return SequenceResult(package.id, package.version, existing.task_id, ordinal)
        item = await self._uow.session.scalar(
            select(PlanRevisionItem).where(
                PlanRevisionItem.plan_revision_id == revision.id,
                PlanRevisionItem.work_package_id == package.id,
                PlanRevisionItem.ordinal == ordinal,
            )
        )
        if item is None:
            package.status = WorkPackageStatus.COMPLETED
            package.cursor_ordinal = None
            package.pause_reason = None
            package.last_failure_task_id = None
            package.version += 1
            discussion = await self._uow.session.get(
                ProjectDiscussionSession, package.session_id, with_for_update=True
            )
            if discussion is not None and discussion.active_work_package_id == package.id:
                discussion.active_work_package_id = None
            await self._uow.events.append(
                entity_type="work_package",
                entity_id=package.id,
                event_type=WorkPackageEvent.PACKAGE_COMPLETED.value,
                actor_type="system",
                previous_state=WorkPackageStatus.RUNNING.value,
                new_state=WorkPackageStatus.COMPLETED.value,
                payload={"revision_id": str(revision.id)},
            )
            await self._projection(package.id, package.version, "completed")
            return SequenceResult(package.id, package.version, None, None, completed=True)

        discussion = await self._uow.session.get(ProjectDiscussionSession, package.session_id)
        if discussion is None:
            raise DomainError("discussion_session_missing")
        if revision.approved_by_user_id is None:
            raise DomainError("approval_provenance_missing")
        draft = _task_draft(package.project_id, item)
        original = _original_text(item)
        task_record = await self._uow.tasks.create(
            user_id=revision.approved_by_user_id,
            chat_id=discussion.chat_id,
            thread_id=discussion.message_thread_id,
            project_id=package.project_id,
            original_text=original,
            task_type=draft.task_type.value,
            task_draft=draft.model_dump(mode="json"),
        )
        task = await self._uow.session.get(Task, task_record.id, with_for_update=True)
        assert task is not None
        task.status = TaskStatus.INTERPRETED
        task.interpreter_profile = MATERIALIZER_PROFILE
        task.prompt_version = MATERIALIZER_PROMPT_VERSION
        task.draft_schema_version = TASK_DRAFT_SCHEMA_VERSION
        task.version += 1
        interpretation = Interpretation(
            task_id=task.id,
            original_input_hash=hashlib.sha256(original.encode()).hexdigest(),
            transcript=None,
            task_draft=draft.model_dump(mode="json"),
            profile_id=MATERIALIZER_PROFILE,
            model=MATERIALIZER_MODEL,
            prompt_version=MATERIALIZER_PROMPT_VERSION,
            schema_version=TASK_DRAFT_SCHEMA_VERSION,
        )
        self._uow.session.add(interpretation)
        await self._uow.session.flush()
        await self._uow.work_packages.add_materialization(
            MaterializationLink(
                work_package_id=package.id,
                plan_revision_id=revision.id,
                work_item_draft_id=item.item_id,
                plan_revision_item_id=item.id,
                task_id=task.id,
                ordinal=ordinal,
            )
        )
        await self._uow.outbox.enqueue(
            destination="workflow_dispatch",
            operation_type="dispatch_interpretation",
            entity_type="interpretation",
            entity_id=interpretation.id,
            idempotency_key=f"workflow:dispatch:{interpretation.id}",
            payload={
                "task_id": str(task.id),
                "work_package_id": str(package.id),
                "ordinal": ordinal,
            },
        )
        await self._uow.events.append(
            entity_type="work_package",
            entity_id=package.id,
            event_type=WorkPackageEvent.PACKAGE_ITEM_MATERIALIZED.value,
            actor_type="system",
            payload={"revision_id": str(revision.id), "ordinal": ordinal, "task_id": str(task.id)},
        )
        await self._projection(package.id, package.version, "materialized")
        return SequenceResult(package.id, package.version, task.id, ordinal)

    async def _fenced_approved_revision(
        self, package_id: uuid.UUID, *, revision_number: int, h8: str
    ) -> PlanRevision:
        try:
            revision = await self._uow.work_packages.get_fenced_revision(
                package_id=package_id, revision_number=revision_number, h8=h8
            )
        except LookupError as exc:
            raise DomainError("stale_revision") from exc
        if revision.state is not PlanRevisionState.APPROVED:
            raise DomainError("approval_binding_mismatch")
        return revision

    async def _projection(self, package_id: uuid.UUID, generation: int, reason: str) -> None:
        if reason == "completed":
            await self._uow.outbox.enqueue(
                destination="work_package_projection",
                operation_type="repost_plan",
                entity_type="work_package",
                entity_id=package_id,
                idempotency_key=f"wp:repost:{package_id}:{generation}:{reason}",
                payload={"package_id": str(package_id)},
            )
        await self._uow.outbox.enqueue(
            destination="work_package_projection",
            operation_type="render_status",
            entity_type="work_package",
            entity_id=package_id,
            idempotency_key=f"wp:projection:sequence:{package_id}:{generation}:{reason}",
            payload={"package_id": str(package_id)},
        )
        await self._uow.outbox.enqueue(
            destination="work_package_projection",
            operation_type="render_action" if reason == "completed" else "clear_action",
            entity_type="work_package",
            entity_id=package_id,
            idempotency_key=f"wp:action:{package_id}:{generation}:{reason}",
            payload={"package_id": str(package_id)},
        )

    async def _task_projection(self, task: Task, generation: int, reason: str) -> None:
        if task.source_chat_id is None or task.source_thread_id is None:
            return
        await self._uow.outbox.enqueue(
            destination="telegram",
            operation_type="send_message",
            entity_type="task",
            entity_id=task.id,
            idempotency_key=f"telegram:task-status:package:{task.id}:{generation}:{reason}",
            payload={
                "chat_id": task.source_chat_id,
                "message_thread_id": task.source_thread_id,
                "role": "intake_ack",
                "task_id": str(task.id),
            },
        )


def _task_draft(project_id: str, item: PlanRevisionItem) -> TaskDraft:
    complexity = {
        EstimatedComplexity.SMALL: SuggestedComplexity.SMALL,
        EstimatedComplexity.MEDIUM: SuggestedComplexity.MEDIUM,
        EstimatedComplexity.LARGE: SuggestedComplexity.LARGE,
    }[item.estimated_complexity]
    constraints = (
        f"Allowed scope: {item.allowed_scope}",
        *(f"Out of scope: {value}" for value in item.out_of_scope),
        *(f"Dependency: {value}" for value in item.dependencies),
        *(f"Trusted check: {value}" for value in item.trusted_checks),
    )
    return TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        project_id=project_id,
        goal=item.goal,
        task_summary=item.summary,
        requested_outcomes=(item.expected_outcome, *tuple(item.completion_criteria)),
        constraints=constraints,
        required_capabilities=frozenset(
            {Capability.REPOSITORY_READ, Capability.FILESYSTEM_WRITE, Capability.CODE_EDIT}
        ),
        suggested_complexity=complexity,
        suggested_risk=item.suggested_risk,
        needs_planning=complexity is SuggestedComplexity.LARGE,
        needs_clarification=False,
        normalized_title=item.summary[:120],
    )


def _original_text(item: PlanRevisionItem) -> str:
    criteria = "\n".join(f"- {value}" for value in item.completion_criteria)
    return f"{item.summary}\n\n{item.goal}\n\nCompletion criteria:\n{criteria}"


class WorkPackageSequenceConsumer:
    """Durably consumes canonical Task terminal evidence."""

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        enabled: bool,
    ) -> None:
        self._settings = settings
        self._factory = session_factory
        self._owner = owner
        self._enabled = enabled

    async def process_one(self) -> bool:
        if not self._enabled:
            return False
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._settings.workflow.lease_seconds,
                allowed_destinations=frozenset({"work_package_sequence"}),
            )
        if token is None:
            return False
        try:
            async with UnitOfWork(self._factory) as uow:
                assert uow.session is not None
                item = await uow.session.get(TransactionalOutbox, token.item_id)
                if (
                    item is None
                    or item.operation_type != "observe_task_terminal"
                    or item.linked_entity_type != "task"
                ):
                    raise ValueError("invalid_sequence_item")
                await WorkPackageSequencer(uow).observe_terminal(task_id=item.linked_entity_id)
                await complete_outbox_item(uow.session, token)
        except (DomainError, ValueError) as error:
            category = str(error)
            if category not in {
                "invalid_sequence_item",
                "materialized_task_missing",
                "task_not_terminal",
            }:
                raise
            async with self._factory.begin() as session:
                await dead_letter_outbox_item(
                    session,
                    token,
                    error_category=f"work_package_sequence_{category}",
                )
        return True
