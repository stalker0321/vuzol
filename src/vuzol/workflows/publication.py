"""Short, locked admission boundary for publisher effects."""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.leadership.evidence import new_effect_blocker
from vuzol.storage.models import Approval, MaterializationLink, Run, Step, Task, WorkPackage
from vuzol.storage.types import ApprovalStatus, RunStatus, StepStatus, TaskStatus
from vuzol.workflows.locking import lock_task_run_step
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import RESULT_APPROVAL_TTL, envelope_hash, verified_envelope

PRODUCTION_APPROVAL_SCHEMA = "production-publication-approval.v1"


class PublicationDenied(RuntimeError):
    """The pending publication no longer has authority."""


def _check_execution(
    task: Task,
    run: Run,
    step: Step,
    request: StepExecutionRequest,
    cancellation: CancellationContext,
) -> None:
    if (
        task.id != request.task_id
        or run.id != request.run_id
        or step.step_type != request.step_type
        or run.status is not RunStatus.RUNNING
        or task.status not in {TaskStatus.EXECUTING, TaskStatus.VALIDATING, TaskStatus.REVIEWING}
        or step.status is not StepStatus.RUNNING
        or step.lease_owner != request.lease.owner
        or step.lease_generation != request.lease.generation
        or step.lease_expires_at is None
        or step.lease_expires_at <= datetime.now(UTC)
    ):
        raise PublicationDenied("publication_lease_lost")
    if cancellation.requested:
        raise PublicationDenied("cancelled")


def _production_envelope(
    task: Task, run: Run, step: Step, target: dict[str, Any]
) -> dict[str, Any]:
    return {
        **target,
        "schema_version": PRODUCTION_APPROVAL_SCHEMA,
        "requested_action": "publish_static",
        "task_id": str(task.id),
        "run_id": str(run.id),
        "step_id": str(step.id),
        "project_id": task.project_id,
        "configuration_revision": run.configuration_revision,
        "policy_revision": run.policy_revision,
    }


async def _production_approval(
    session: AsyncSession,
    task: Task,
    run: Run,
    step: Step,
    target: dict[str, Any],
    *,
    create: bool = False,
) -> Approval:
    if step.step_type != "publish_static":
        raise PublicationDenied("production_step_mismatch")
    managed = await session.scalar(
        select(WorkPackage.managed_goal_id)
        .join(MaterializationLink, MaterializationLink.work_package_id == WorkPackage.id)
        .where(MaterializationLink.task_id == task.id)
    )
    if managed is not None:
        raise PublicationDenied("managed_integration_only")
    if not all(
        isinstance(target.get(key), str) and target[key]
        for key in ("source_commit", "artifact_hash", "destination", "public_url")
    ):
        raise PublicationDenied("production_target_missing")
    envelope = _production_envelope(task, run, step, target)
    approval = await session.scalar(
        select(Approval).where(Approval.step_id == step.id).with_for_update()
    )
    if approval is None and create:
        digest = envelope_hash(envelope)
        approval = Approval(
            id=uuid.uuid4(),
            step_id=step.id,
            action_envelope_hash=digest,
            requested_action="publish_static",
            normalized_target=target["destination"],
            human_summary="Публикация проверенного артефакта в production.",
            token_hash=envelope_hash({"step_id": str(step.id), "digest": digest}),
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(UTC) + RESULT_APPROVAL_TTL,
        )
        session.add(approval)
        step.payload = {
            **step.payload,
            "approval_id": str(approval.id),
            "action_envelope": envelope,
        }
        await session.flush()
    if approval is None:
        raise PublicationDenied("production_approval_required")
    try:
        stored = verified_envelope(step, approval)
    except ValueError as error:
        raise PublicationDenied("production_approval_changed") from error
    if (
        stored != envelope
        or approval.requested_action != "publish_static"
        or approval.normalized_target != target["destination"]
        or approval.expires_at <= datetime.now(UTC)
    ):
        raise PublicationDenied("production_approval_changed")
    if approval.status is ApprovalStatus.PENDING and create:
        return approval
    if (
        approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}
        or approval.deciding_user_id != task.user_id
    ):
        raise PublicationDenied("production_owner_approval_required")
    return approval


async def ensure_production_approval(
    factory: async_sessionmaker[AsyncSession],
    request: StepExecutionRequest,
    cancellation: CancellationContext,
    target: dict[str, Any],
) -> bool:
    """Ask for human consent without granting any effect authority."""
    async with factory.begin() as session:
        locked = await lock_task_run_step(session, request.step_id)
        if locked is None:
            raise PublicationDenied("publication_workflow_missing")
        task, run, step = locked
        _check_execution(task, run, step, request, cancellation)
        approval = await _production_approval(session, task, run, step, target, create=True)
        return approval.status in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}


@asynccontextmanager
async def publication_authority(
    factory: async_sessionmaker[AsyncSession],
    request: StepExecutionRequest,
    cancellation: CancellationContext,
    *,
    feature_enabled: bool,
    restoring: dict[str, object] | None = None,
    production_target: dict[str, Any] | None = None,
) -> AsyncIterator[None]:
    # Goal -> Task -> Run -> Step is also the owner-control lock order.
    async with factory.begin() as session:
        blocker = await new_effect_blocker(
            session,
            request.task_id,
            feature_enabled=feature_enabled,
            restoring_publication=restoring is not None,
        )
        if blocker is not None:
            raise PublicationDenied(blocker)
        locked = await lock_task_run_step(session, request.step_id)
        if locked is None:
            raise PublicationDenied("publication_workflow_missing")
        task, run, step = locked
        if step.step_type != ("publish_preview" if production_target is None else "publish_static"):
            raise PublicationDenied("publication_target_mismatch")
        if restoring is not None:
            # Recovery of an already published artifact is not a new step lease.
            # Revalidate the exact receipt and newest publication under control locks.
            latest = await session.scalar(
                select(Step.id)
                .join(Run, Run.id == Step.run_id)
                .join(Task, Task.id == Run.task_id)
                .where(
                    Task.project_id == task.project_id,
                    Step.step_type == "publish_preview",
                    Step.status == StepStatus.COMPLETED,
                )
                .order_by(Step.updated_at.desc(), Step.id.desc())
                .limit(1)
            )
            if (
                step.id != latest
                or step.result != restoring
                or step.status is not StepStatus.COMPLETED
                or task.status in {TaskStatus.PAUSED, TaskStatus.CANCELLED, TaskStatus.ROLLED_BACK}
                or run.status in {RunStatus.PAUSED, RunStatus.CANCELLED}
                or task.id != request.task_id
                or run.id != request.run_id
                or cancellation.requested
            ):
                raise PublicationDenied("preview_restore_stale")
            yield
            return
        _check_execution(task, run, step, request, cancellation)
        approval = (
            await _production_approval(session, task, run, step, production_target)
            if production_target is not None
            else None
        )
        # Permission lookup can wait on a row lock beyond the lease deadline.
        # No await is allowed between this final clock/cancellation check and yield.
        _check_execution(task, run, step, request, cancellation)
        yield
        if approval is not None:
            # Exact same step/artifact can replay after a crash before workflow ack.
            approval.status = ApprovalStatus.CONSUMED
            approval.consumed_at = datetime.now(UTC)
