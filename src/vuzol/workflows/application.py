"""Transport-neutral task application service (WP09).

Identical user actions over Telegram and CLI/API: commands run against the
domain operations with an explicit principal (no fake chat/user IDs) and a
task-level CAS (stale revision never applies a control). Inspect is strictly
read-only: no locks, no writes, no outbox rows.

Both Telegram ingress (`workflows/controls._apply`, `workflows/dispatch`)
and the operator CLI route through `apply_task_command` here.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.providers.budgets import (
    _explicit_money_family,
    accept_cost_uncertainty,
    reconcile_unknown_usage,
    record_admission_only_consent,
)
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    Run,
    Step,
    Task,
    UsageRecord,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import RunStatus, StepStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.acceptance import (
    criterion_choice_token,
    load_stored_proofs,
    record_owner_criterion_acceptance,
    required_criterion_ids,
    unproven_criterion_ids,
)
from vuzol.workflows.controls import (
    _locked_context,
    cancel_task,
    pause_task,
    recover_unsent_cli_review,
    recover_validation_environment,
    resume_task,
    retry_blocked_step,
)
from vuzol.workflows.retry_policy import current_retry_target
from vuzol.workflows.service import _enqueue_telegram_projection, start_run

INGRESS_SOURCES = frozenset({"telegram", "cli", "api"})


class TaskCommand(StrEnum):
    START = "start"
    PAUSE = "pause"
    CANCEL = "cancel"
    RESUME = "resume"
    INSPECT = "inspect"
    RETRY = "retry"
    REVALIDATE = "revalidate"
    REREVIEW = "rereview"
    ADMIT_CEILING = "admit_ceiling"
    ACCEPT_UNCERTAINTY = "accept_uncertainty"
    RECONCILE_COST = "reconcile_cost"
    ACCEPT_CRITERION = "accept_criterion"


_OWNER_CATEGORY = {
    TaskCommand.ADMIT_CEILING: "admission_only_consent_required",
    TaskCommand.ACCEPT_UNCERTAINTY: "money_cap_uncertified",
    TaskCommand.RECONCILE_COST: "money_cap_uncertified",
    TaskCommand.ACCEPT_CRITERION: "acceptance_evidence_missing",
}


@dataclass(frozen=True, slots=True)
class OwnerDecision:
    """Stored owner choice that lets one blocked step continue."""

    reason: str
    cost_units: Decimal | None = None
    usage_id: uuid.UUID | None = None
    criterion_id: str | None = None
    criterion_token: str | None = None


@dataclass(frozen=True, slots=True)
class Principal:
    """Explicit caller identity. Zero IDs are rejected, never defaulted."""

    user_id: int
    ingress_source: str


@dataclass(frozen=True, slots=True)
class TaskCommandResult:
    task_id: uuid.UUID
    version: int
    status: str
    applied: bool


def validate_principal(principal: Principal) -> None:
    if principal.user_id == 0:
        raise ValueError("principal_invalid: refusing fake user ID 0")
    if principal.ingress_source not in INGRESS_SOURCES:
        raise ValueError(f"principal_invalid: unknown ingress {principal.ingress_source}")


def validate_command(command: object) -> TaskCommand:
    try:
        return TaskCommand(command)  # type: ignore[arg-type]
    except ValueError as error:
        raise ValueError(f"unknown task command: {command}") from error


async def _read_version(session: AsyncSession, task_id: uuid.UUID) -> int:
    task = await session.get(Task, task_id)
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    return task.version


async def _start(
    session: AsyncSession,
    task_id: uuid.UUID,
    actor_id: str,
    expected_task_version: int | None,
) -> bool:
    task, run, _ = await _locked_context(session, task_id)
    if expected_task_version is not None and task.version != expected_task_version:
        raise ValueError(f"stale task version: expected {expected_task_version}")
    before = task.version
    await start_run(session, run, task=task, actor_type="user", actor_id=actor_id)
    if task.version != before:
        await _enqueue_telegram_projection(session, task, run)
    return task.version != before


async def _unknown_usage_id(
    session: AsyncSession, task_id: uuid.UUID, usage_id: uuid.UUID | None
) -> uuid.UUID:
    """One uncovered receipt, or the id the owner named. Never invent an amount."""

    family = await _explicit_money_family(session, task_id)
    rows = (
        await session.scalars(
            select(UsageRecord.id).where(
                UsageRecord.task_id.in_(family),
                UsageRecord.cost_known.is_(False),
            )
        )
    ).all()
    if usage_id is not None:
        if usage_id not in rows:
            raise ValueError("unknown usage receipt is missing")
        return usage_id
    if len(rows) != 1:
        raise ValueError("reconciliation needs one unknown receipt or an explicit usage id")
    return rows[0]


async def _accept_named_criterion(
    session: AsyncSession,
    *,
    task: Task,
    run_id: uuid.UUID,
    principal: Principal,
    decision: OwnerDecision,
) -> None:
    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    package = None if link is None else await session.get(WorkPackage, link.work_package_id)
    if package is None:
        raise ValueError("criterion acceptance requires a work package")
    worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
    if worktree is None or not worktree.result_commit:
        raise ValueError("criterion acceptance requires the retained result commit")
    proofs = await load_stored_proofs(session, package_id=package.id)
    missing = unproven_criterion_ids(
        result_commit=worktree.result_commit,
        required_ids=required_criterion_ids(package.exit_criteria),
        gates=proofs.gates,
        findings=proofs.findings,
        owner_decisions=proofs.owner_decisions,
    )
    chosen = decision.criterion_id
    if chosen is None and decision.criterion_token:
        matches = [
            criterion_id
            for criterion_id in missing
            if criterion_choice_token(criterion_id) == decision.criterion_token
        ]
        if len(matches) != 1:
            raise ValueError("criterion token does not match one unproven criterion")
        chosen = matches[0]
    elif chosen is None:
        if len(missing) != 1:
            raise ValueError("criterion acceptance needs exactly one unproven criterion")
        chosen = missing[0]
    elif chosen not in missing:
        raise ValueError("criterion is not an unproven required criterion")
    await record_owner_criterion_acceptance(
        session,
        package_id=package.id,
        criterion_id=chosen,
        result_commit=worktree.result_commit,
        principal_user_id=principal.user_id,
        reason=decision.reason,
    )


async def _continue_owner_decision(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    action: TaskCommand,
    principal: Principal,
    expected_task_version: int | None,
    step_id: uuid.UUID | None,
    decision: OwnerDecision | None,
) -> None:
    """Permission, stored decision, then the same blocked step continues."""

    if decision is None or not decision.reason.strip():
        raise ValueError("owner decision requires a reason")
    task, run, steps = await _locked_context(session, task_id)
    if task.user_id != principal.user_id:
        raise ValueError("task continuation requires the task owner's principal")
    if expected_task_version is None or expected_task_version != task.version:
        raise ValueError("stale task version for continuation")
    if run.status is RunStatus.PAUSED:
        raise ValueError("run is paused; resume to continue the saved work or cancel to close it")
    target = current_retry_target(steps)
    if (
        run.status is not RunStatus.BLOCKED
        or target is None
        or (step_id is not None and target.id != step_id)
    ):
        raise ValueError("continuation requires one unambiguous current blocked step")
    if target.failure_category != _OWNER_CATEGORY[action]:
        raise ValueError("owner action does not match the blocked step")
    if action is TaskCommand.ADMIT_CEILING:
        await record_admission_only_consent(
            session,
            task_id=task.id,
            principal_user_id=principal.user_id,
            reason=decision.reason,
        )
    elif action is TaskCommand.ACCEPT_UNCERTAINTY:
        await accept_cost_uncertainty(
            session,
            task_id=task.id,
            principal_user_id=principal.user_id,
            reason=decision.reason,
        )
    elif action is TaskCommand.RECONCILE_COST:
        if decision.cost_units is None or decision.cost_units <= 0:
            raise ValueError("reconciliation requires a measured receipt")
        usage_id = await _unknown_usage_id(session, task.id, decision.usage_id)
        await reconcile_unknown_usage(
            session,
            usage_id=usage_id,
            cost_units=decision.cost_units,
            principal_user_id=principal.user_id,
        )
    else:
        await _accept_named_criterion(
            session,
            task=task,
            run_id=run.id,
            principal=principal,
            decision=decision,
        )
    await retry_blocked_step(session, target.id, actor_id=str(principal.user_id))


async def continue_blocked_receipt(
    session: AsyncSession,
    *,
    chat_id: int,
    thread_id: int,
    principal: Principal,
    cost_units: Decimal,
) -> TaskCommandResult:
    """Reconcile the single owner-blocked unknown receipt in this thread."""

    validate_principal(principal)
    if cost_units <= 0:
        raise ValueError("a measured receipt is not zero")
    tasks = (
        await session.scalars(
            select(Task)
            .join(Run, Run.task_id == Task.id)
            .join(Step, Step.run_id == Run.id)
            .where(
                Task.user_id == principal.user_id,
                Task.source_chat_id == chat_id,
                Task.source_thread_id == thread_id,
                Run.status == RunStatus.BLOCKED,
                Step.status == StepStatus.BLOCKED,
                Step.failure_category == "money_cap_uncertified",
            )
        )
    ).all()
    unique = {task.id: task for task in tasks}
    if len(unique) != 1:
        raise ValueError("receipt requires exactly one blocked unknown-cost task in this thread")
    task = next(iter(unique.values()))
    return await apply_task_command(
        session,
        task_id=task.id,
        command=TaskCommand.RECONCILE_COST,
        principal=principal,
        expected_task_version=task.version,
        decision=OwnerDecision(reason="late receipt from the owner", cost_units=cost_units),
    )


async def apply_task_command(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    command: TaskCommand | str,
    principal: Principal,
    expected_task_version: int | None = None,
    step_id: uuid.UUID | None = None,
    decision: OwnerDecision | None = None,
) -> TaskCommandResult:
    """Session-level core shared by Telegram ingress, dispatch and the CLI."""

    action = validate_command(command)
    validate_principal(principal)
    actor_id = str(principal.user_id)
    if action is TaskCommand.INSPECT:
        task = await session.get(Task, task_id)
        if task is None:
            raise ValueError(f"task not found: {task_id}")
        return TaskCommandResult(
            task_id=task.id,
            version=task.version,
            status=task.status.value,
            applied=False,
        )
    if action is TaskCommand.REREVIEW:
        if principal.ingress_source != "cli":
            raise ValueError("review recovery requires an explicit operator CLI request")
        await recover_unsent_cli_review(
            session,
            task_id=task_id,
            user_id=principal.user_id,
            expected_task_version=expected_task_version,
            step_id=step_id,
        )
        applied = True
    elif action is TaskCommand.REVALIDATE:
        if principal.ingress_source != "cli":
            raise ValueError("environment recovery requires an explicit operator CLI request")
        await recover_validation_environment(
            session,
            task_id=task_id,
            user_id=principal.user_id,
            expected_task_version=expected_task_version,
            step_id=step_id,
        )
        applied = True
    elif action is TaskCommand.RETRY:
        task, run, steps = await _locked_context(session, task_id)
        if task.user_id != principal.user_id:
            raise ValueError("task retry requires the task owner's principal")
        if expected_task_version is None or expected_task_version != task.version:
            raise ValueError("stale task version for retry")
        if run.status is RunStatus.PAUSED:
            raise ValueError(
                "run is paused; resume to continue the saved work or cancel to close it"
            )
        target = current_retry_target(steps)
        if (
            run.status is not RunStatus.BLOCKED
            or target is None
            or (step_id is not None and target.id != step_id)
        ):
            raise ValueError("retry requires one unambiguous current blocked step")
        await retry_blocked_step(session, target.id, actor_id=actor_id)
        session.add(
            Event(
                entity_type="task",
                entity_id=task.id,
                event_type="task.retry_requested",
                actor_type="user",
                actor_id=actor_id,
                payload={
                    "ingress_source": principal.ingress_source,
                    "step_id": str(target.id),
                    "expected_task_version": expected_task_version,
                },
            )
        )
        applied = True
    elif action in _OWNER_CATEGORY:
        await _continue_owner_decision(
            session,
            task_id=task_id,
            action=action,
            principal=principal,
            expected_task_version=expected_task_version,
            step_id=step_id,
            decision=decision,
        )
        applied = True
    elif action is TaskCommand.START:
        applied = await _start(session, task_id, actor_id, expected_task_version)
    elif action is TaskCommand.PAUSE:
        before = await _read_version(session, task_id)
        await pause_task(
            session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
        )
        applied = (await _read_version(session, task_id)) != before
    elif action is TaskCommand.RESUME:
        before = await _read_version(session, task_id)
        await resume_task(
            session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
        )
        applied = (await _read_version(session, task_id)) != before
    else:
        before = await _read_version(session, task_id)
        await cancel_task(
            session, task_id, actor_id=actor_id, expected_task_version=expected_task_version
        )
        applied = (await _read_version(session, task_id)) != before
    task = await session.get(Task, task_id)
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    return TaskCommandResult(
        task_id=task.id,
        version=task.version,
        status=task.status.value,
        applied=applied,
    )


class TaskControlService:
    """Application boundary for task commands; Telegram-free, CLI-ready."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._factory = session_factory

    async def execute(
        self,
        *,
        task_id: uuid.UUID,
        command: TaskCommand | str,
        principal: Principal,
        expected_task_version: int | None = None,
        step_id: uuid.UUID | None = None,
        decision: OwnerDecision | None = None,
    ) -> TaskCommandResult:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            return await apply_task_command(
                uow.session,
                task_id=task_id,
                command=command,
                principal=principal,
                expected_task_version=expected_task_version,
                step_id=step_id,
                decision=decision,
            )
