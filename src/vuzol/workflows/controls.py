"""Persisted workflow control semantics."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Settings
from vuzol.providers.budgets import ADMISSION_ONLY_CONSENT_SUMMARY
from vuzol.providers.fallback_policy import should_fallback_provider
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item
from vuzol.storage.models import (
    Approval,
    Event,
    Run,
    Step,
    SupervisedProcess,
    Task,
    TelegramControlAction,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    USER_TERMINAL_TASK_STATUSES,
    ApprovalStatus,
    ControlActionStatus,
    ProcessStatus,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.workflows.locking import lock_task_run_step
from vuzol.workflows.result_approval import verified_envelope
from vuzol.workflows.retry_policy import (
    LEASE_EXPIRED_ATTEMPTS_EXHAUSTED,
    blocked_step_is_retryable,
    repeatable_without_new_mutation,
)
from vuzol.workflows.service import (
    _enqueue_telegram_projection,
    activate_ready_steps,
    derive_task_status,
)
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

if TYPE_CHECKING:
    from vuzol.workflows.application import TaskCommandResult

RUN_TERMINAL = {RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.COMPLETED}
STEP_TERMINAL = {StepStatus.FAILED, StepStatus.CANCELLED, StepStatus.COMPLETED}
_TELEGRAM_OWNER_ACTIONS = {
    "admit": ("admit_ceiling", ADMISSION_ONLY_CONSENT_SUMMARY),
    "uncertain": (
        "accept_uncertainty",
        "owner accepted the current unknown cost from the status card",
    ),
}


def _command_outcome(result: TaskCommandResult) -> dict[str, object]:
    """JSON-safe receipt for a task command result (D1 L5)."""

    payload = asdict(result)
    return {
        key: (str(value) if isinstance(value, uuid.UUID) else value)
        for key, value in payload.items()
    }


class WorkflowControlConsumer:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
    ) -> None:
        self._settings = settings
        self._factory = session_factory
        self._owner = owner

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._settings.workflow.lease_seconds,
                allowed_destinations=frozenset({"workflow_control"}),
            )
        if token is None:
            return False
        async with self._factory.begin() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            if item is None or item.linked_entity_type != "telegram_control_action":
                raise ValueError("workflow control outbox item is invalid")
            action = await session.scalar(
                select(TelegramControlAction)
                .where(TelegramControlAction.id == item.linked_entity_id)
                .with_for_update()
            )
            if action is None:
                raise ValueError("workflow control action is missing")
            try:
                async with session.begin_nested():
                    await self._apply(session, action)
            except ValueError as error:
                action.status = ControlActionStatus.REJECTED
                action.payload = {**action.payload, "rejection_reason": str(error)[:500]}
                if (
                    action.task_id is not None
                    and action.payload.get("expected_task_version") is not None
                ):
                    task = await session.get(Task, action.task_id)
                    if (
                        task is not None
                        and task.source_chat_id is not None
                        and task.source_thread_id is not None
                    ):
                        from vuzol.telegram.projections import enqueue_task_control_snapshot

                        await enqueue_task_control_snapshot(
                            session,
                            task=task,
                            chat_id=task.source_chat_id,
                            thread_id=task.source_thread_id,
                            user_id=action.requested_by_user_id,
                            key=f"task:control-rejected:{action.id}",
                        )
            else:
                action.status = ControlActionStatus.PROCESSED
            action.processed_at = func.now()
            await complete_outbox_item(session, token)
        return True

    async def _apply(self, session: AsyncSession, action: TelegramControlAction) -> None:
        from vuzol.workflows.application import Principal, apply_task_command

        if action.action_kind in {"approve", "redo", "reject"}:
            if action.approval_id is None:
                raise ValueError("result decision requires an approval target")
            await decide_result(
                session,
                action.approval_id,
                decision=action.action_kind,
                deciding_user_id=action.requested_by_user_id,
            )
            return
        if action.action_kind in {"retry", "admit", "uncertain"}:
            if action.step_id is None:
                raise ValueError("retry requires a step target")
            if action.status is not ControlActionStatus.QUEUED:
                return
            stored_step = await session.get(Step, action.step_id)
            stored_run = None if stored_step is None else await session.get(Run, stored_step.run_id)
            if stored_run is None or (
                action.task_id is not None and action.task_id != stored_run.task_id
            ):
                raise ValueError("retry task/step binding is invalid")
            target_task_id = stored_run.task_id
            version = action.payload.get("expected_task_version")
            if version is None:
                # Compatibility for already queued legacy step-target receipts.
                # New Telegram retries require a version at ingress.
                stored_task = await session.get(Task, target_task_id)
                if stored_task is None:
                    raise ValueError("retry task is missing")
                version = stored_task.version
            if action.action_kind == "retry":
                result = await apply_task_command(
                    session,
                    task_id=target_task_id,
                    command="retry",
                    principal=Principal(action.requested_by_user_id, "telegram"),
                    expected_task_version=version,
                    step_id=action.step_id,
                )
            else:
                from vuzol.workflows.application import OwnerDecision

                command, reason = _TELEGRAM_OWNER_ACTIONS[action.action_kind]
                result = await apply_task_command(
                    session,
                    task_id=target_task_id,
                    command=command,
                    principal=Principal(action.requested_by_user_id, "telegram"),
                    expected_task_version=version,
                    step_id=action.step_id,
                    decision=OwnerDecision(reason=reason),
                )
            action.payload = {**action.payload, "outcome": _command_outcome(result)}
            return
        if action.task_id is None:
            raise ValueError("workflow control requires a task target")
        # D1 L5: task commands record the apply receipt on the action so a
        # duplicate delivery can return the same outcome (package-controls
        # parity: discussion/application _duplicate_result).
        if action.action_kind == "pause":
            result = await apply_task_command(
                session,
                task_id=action.task_id,
                command="pause",
                principal=Principal(action.requested_by_user_id, "telegram"),
                expected_task_version=action.payload.get("expected_task_version"),
            )
            action.payload = {**action.payload, "outcome": _command_outcome(result)}
            return
        if action.action_kind == "resume":
            result = await apply_task_command(
                session,
                task_id=action.task_id,
                command="resume",
                principal=Principal(action.requested_by_user_id, "telegram"),
                expected_task_version=action.payload.get("expected_task_version"),
            )
            action.payload = {**action.payload, "outcome": _command_outcome(result)}
            return
        if action.action_kind == "cancel":
            result = await apply_task_command(
                session,
                task_id=action.task_id,
                command="cancel",
                principal=Principal(action.requested_by_user_id, "telegram"),
                expected_task_version=action.payload.get("expected_task_version"),
            )
            action.payload = {**action.payload, "outcome": _command_outcome(result)}
            return
        if action.action_kind == "start":
            result = await apply_task_command(
                session,
                task_id=action.task_id,
                command="start",
                principal=Principal(action.requested_by_user_id, "telegram"),
                expected_task_version=action.payload.get("expected_task_version"),
            )
            action.payload = {**action.payload, "outcome": _command_outcome(result)}
            return
        raise ValueError(f"unsupported workflow control: {action.action_kind}")


async def decide_result(
    session: AsyncSession,
    approval_id: uuid.UUID,
    *,
    decision: str,
    deciding_user_id: int,
) -> None:
    step_id = await session.scalar(select(Approval.step_id).where(Approval.id == approval_id))
    locked = await lock_task_run_step(session, step_id) if step_id is not None else None
    if locked is None:
        raise ValueError("result approval workflow is missing")
    task, run, step = locked
    approval = await session.scalar(
        select(Approval).where(Approval.id == approval_id).with_for_update()
    )
    if approval is None or approval.status is not ApprovalStatus.PENDING:
        raise ValueError("result approval is missing or already decided")
    if approval.expires_at <= datetime.now(UTC):
        raise ValueError("result approval has expired")
    if step.status is not StepStatus.WAITING_APPROVAL:
        raise ValueError("result approval step is not waiting for a decision")
    verified_envelope(step, approval)
    # D2 L4 correction barrier: the envelope pins the revisions it was
    # requested under; drift between request and decision fails closed
    # (inv.35 — any material change invalidates the approval). Legacy
    # envelopes without the fields pass through (Q4 compat).
    envelope = step.payload.get("action_envelope")
    if isinstance(envelope, dict):
        for field in ("configuration_revision", "policy_revision"):
            expected = envelope.get(field)
            if expected is not None and expected != getattr(run, field):
                raise ValueError(f"approval envelope {field} drifted since request")
    # Existing result/install controls retain their allowlisted-operator contract.
    # Production introduces a stricter, task-owner-only grant.
    if approval.requested_action == "publish_static" and deciding_user_id != task.user_id:
        raise ValueError("result approval requires the task owner")
    approval.deciding_user_id = deciding_user_id
    approval.decided_at = func.now()
    actor_id = str(deciding_user_id)
    installation_action = approval.requested_action in {
        "install_capabilities",
        "install_dependencies",
    }
    publication_action = approval.requested_action == "publish_static"
    installation_event = (
        "capability_installation"
        if approval.requested_action == "install_capabilities"
        else "dependency_installation"
    )
    if decision == "approve":
        approval.status = ApprovalStatus.APPROVED
        if (installation_action or publication_action) and step.attempt_count > 0:
            # Inspecting needs and asking for consent performed no installation.
            # The approved install must remain claimable even with max_attempts=1.
            step.attempt_count -= 1
        await transition_step(
            session, step, StepStatus.QUEUED, actor_type="user", actor_id=actor_id
        )
        await transition_task(
            session, task, TaskStatus.EXECUTING, actor_type="user", actor_id=actor_id
        )
        event_type = f"{installation_event}.approved" if installation_action else "result.approved"
    elif decision == "redo":
        if installation_action or publication_action:
            raise ValueError("effect approval does not support redo")
        # D1 L6: REDO opens a new work attempt of the same logical item and
        # links the superseded candidate/review — it never erases them.
        prior_candidate_hash: str | None = None
        prior_review_summary: str | None = None
        if isinstance(step.result, dict):
            import hashlib as _hashlib
            import json as _json

            prior_candidate_hash = _hashlib.sha256(
                _json.dumps(step.result, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            summary = step.result.get("summary") or step.result.get("implementation_summary")
            if isinstance(summary, str) and summary.strip():
                prior_review_summary = summary.strip()[:2000]
        from vuzol.storage.attempts import (
            latest_attempt,
            record_work_attempt,
            resolve_stable_item,
        )

        previous = await latest_attempt(session, step.id)
        await record_work_attempt(
            session,
            task_id=task.id,
            run_id=run.id,
            step_id=step.id,
            attempt_kind="retry",
            purpose="coding",
            parent_attempt_id=previous.id if previous is not None else None,
            stable_item_id=await resolve_stable_item(session, task.id),
            intent_revision=task.spec_revision,
            outcome="cancelled",
            prior_candidate_hash=prior_candidate_hash,
            prior_review_summary=prior_review_summary,
        )
        approval.status = ApprovalStatus.REJECTED
        await transition_step(
            session, step, StepStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        await transition_run(
            session, run, RunStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        await transition_task(
            session, task, TaskStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        event_type = "result.redo_requested"
    elif decision == "reject":
        approval.status = ApprovalStatus.REJECTED
        await transition_step(
            session, step, StepStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        await transition_run(
            session, run, RunStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        await transition_task(
            session, task, TaskStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        event_type = f"{installation_event}.rejected" if installation_action else "result.rejected"
        # D2 L6: a rejected final approval leaves a durable corrective trace
        # instead of silent cancellation.
        from vuzol.workflows.corrective import record_corrective_signal

        await record_corrective_signal(
            session,
            scope="final_reject",
            reason="final_reject",
            task_id=task.id,
            run_id=run.id,
            step_id=step.id,
        )
    else:
        raise ValueError(f"unsupported result decision: {decision}")
    if approval.requested_action == "install_capabilities" and decision == "reject":
        from vuzol.projects.capability_provisioning import wake_capability_waiters

        await wake_capability_waiters(session, approval)
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type=event_type,
            actor_type="user",
            actor_id=actor_id,
            payload={"approval_id": str(approval.id)},
        )
    )
    await _enqueue_telegram_projection(session, task, run)


def _check_task_version(task: Task, expected_task_version: int | None) -> None:
    """Task-level CAS: a stale control never applies (lead decision 2).

    Compares against Task.version under the row lock, mirroring the
    require_generation contract; None preserves the legacy unchecked path.
    """

    if expected_task_version is not None and task.version != expected_task_version:
        raise ValueError(f"stale task version: expected {expected_task_version}")


async def pause_task(
    session: AsyncSession,
    task_id: uuid.UUID,
    *,
    actor_id: str | None = None,
    expected_task_version: int | None = None,
) -> None:
    task, run, steps = await _locked_context(session, task_id)
    _check_task_version(task, expected_task_version)
    if run.status is RunStatus.PAUSED:
        await _noop(session, run.id, "pause", actor_id)
        return
    if run.status in RUN_TERMINAL:
        raise ValueError(f"terminal run cannot pause: {run.status.value}")
    await transition_run(session, run, RunStatus.PAUSED, actor_type="user", actor_id=actor_id)
    await transition_task(session, task, TaskStatus.PAUSED, actor_type="user", actor_id=actor_id)
    active = [step for step in steps if step.status in {StepStatus.LEASED, StepStatus.RUNNING}]
    for step in active:
        was_running = step.status is StepStatus.RUNNING
        # Revoke the lease durably, not just while run.status happens to be paused.
        # A fast resume must never legalize the old process's late result.
        step.unknown_effects = step.unknown_effects or (
            was_running and not repeatable_without_new_mutation(step)
        )
        await transition_step(
            session, step, StepStatus.BLOCKED, actor_type="user", actor_id=actor_id
        )
        step.failure_category = "owner_paused" if was_running else "owner_paused_before_start"
        step.failure_summary = "Owner paused execution; the previous lease is revoked"
        step.lease_owner = None
        step.lease_expires_at = None
        step.lease_generation += 1
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.pause_effective",
            actor_type="user",
            actor_id=actor_id,
            payload={"active_step_ids": [str(step.id) for step in active]},
        )
    )

    await _enqueue_telegram_projection(session, task, run)


async def _continue_paused_step(
    session: AsyncSession,
    task: Task,
    run: Run,
    step: Step,
    *,
    actor_id: str | None,
) -> None:
    """Grant one bounded attempt of the parked step. Completed work stays put."""

    if step.attempt_count >= step.max_attempts:
        step.max_attempts += 1
    await transition_step(session, step, StepStatus.QUEUED, actor_type="user", actor_id=actor_id)
    step.failure_category = None
    step.failure_summary = None
    task.budget_epoch += 1
    run.selected_route = {
        **run.selected_route,
        "recovery_epoch": task.budget_epoch,
        "recovery_epoch_started_at": datetime.now(UTC).isoformat(),
    }
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.budget_epoch_started",
            actor_type="user",
            actor_id=actor_id,
            payload={"budget_epoch": task.budget_epoch, "reason": "paused_step_continuation"},
        )
    )


async def resume_task(
    session: AsyncSession,
    task_id: uuid.UUID,
    *,
    actor_id: str | None = None,
    expected_task_version: int | None = None,
) -> None:
    task, run, steps = await _locked_context(session, task_id)
    _check_task_version(task, expected_task_version)
    if run.status is RunStatus.RUNNING:
        await _noop(session, run.id, "resume", actor_id)
        return
    if run.status is not RunStatus.PAUSED:
        raise ValueError(f"run cannot resume from {run.status.value}")
    if task.status in {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.COMPLETED}:
        raise ValueError(
            f"task is {task.status.value} while its run is paused; no automatic repair is applied"
        )
    continuable = [
        step
        for step in steps
        if step.status is StepStatus.BLOCKED
        and step.failure_category
        in {LEASE_EXPIRED_ATTEMPTS_EXHAUSTED, "owner_paused", "owner_paused_before_start"}
        and not step.unknown_effects
        and (
            repeatable_without_new_mutation(step)
            or step.failure_category == "owner_paused_before_start"
        )
    ]
    undrained = await session.scalar(
        select(SupervisedProcess.id)
        .where(
            SupervisedProcess.run_id == run.id,
            SupervisedProcess.status != ProcessStatus.EXITED,
        )
        .limit(1)
    )
    if undrained is not None:
        raise ValueError("resume requires the previous supervised process to stop")
    unsafe = [step for step in steps if step.status is StepStatus.BLOCKED and step.unknown_effects]
    if unsafe:
        raise ValueError(
            "resume cannot continue a step with unknown external effects; "
            "reconcile the effect or cancel"
        )
    if len(continuable) > 1:
        raise ValueError("resume requires one continuable paused step")
    await transition_run(session, run, RunStatus.RUNNING, actor_type="user", actor_id=actor_id)
    if continuable:
        await _continue_paused_step(session, task, run, continuable[0], actor_id=actor_id)
    await activate_ready_steps(session, run)
    target = derive_task_status(await _steps(session, run.id), run.status)
    if target is TaskStatus.BLOCKED:
        # Resuming an owner pause is not permission to retry an unrelated
        # unresolved blocker. Restore its run projection without losing cause.
        await transition_run(session, run, RunStatus.BLOCKED, actor_type="user", actor_id=actor_id)
    await transition_task(session, task, target, actor_type="user", actor_id=actor_id)
    await _enqueue_telegram_projection(session, task, run)


async def cancel_task(
    session: AsyncSession,
    task_id: uuid.UUID,
    *,
    actor_id: str | None = None,
    expected_task_version: int | None = None,
) -> None:
    if await _cancel_runless(session, task_id, actor_id, expected_task_version):
        return
    task, run, steps = await _locked_context(session, task_id)
    _check_task_version(task, expected_task_version)
    if run.status is RunStatus.CANCELLED:
        await _noop(session, run.id, "cancel", actor_id)
        return
    retain_failure = task.status is TaskStatus.FAILED and run.status is RunStatus.PAUSED
    if task.status is TaskStatus.COMPLETED and run.status is RunStatus.PAUSED:
        raise ValueError(
            f"task is {task.status.value} while its run is paused; no automatic repair is applied"
        )
    if run.status in {RunStatus.FAILED, RunStatus.COMPLETED}:
        raise ValueError(f"terminal run cannot cancel: {run.status.value}")
    for step in steps:
        if step.status in STEP_TERMINAL:
            continue
        if step.status is StepStatus.RUNNING:
            step.unknown_effects = True
        await transition_step(
            session, step, StepStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
        step.lease_owner = None
        step.lease_expires_at = None
    await transition_run(session, run, RunStatus.CANCELLED, actor_type="user", actor_id=actor_id)
    if retain_failure:
        # Legacy inconsistent state: cancellation settles the still-live run;
        # it neither retries work nor rewrites the terminal task failure.
        task.version += 1
        from vuzol.workflows.lead_events import publish_task_event

        await publish_task_event(session, task)
    else:
        await transition_task(
            session, task, TaskStatus.CANCELLED, actor_type="user", actor_id=actor_id
        )
    from vuzol.projects.capability_provisioning import abandon_capability_installation

    for step in steps:
        if step.step_type == "ensure_capabilities":
            await abandon_capability_installation(session, step.id, reason="leader_cancelled")
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.cancel_requested",
            actor_type="user",
            actor_id=actor_id,
            payload={"rollback_attempted": False, "retained_task_failure": retain_failure},
        )
    )
    await _enqueue_telegram_projection(session, task, run)


async def _cancel_runless(
    session: AsyncSession,
    task_id: uuid.UUID,
    actor_id: str | None,
    expected_task_version: int | None,
) -> bool:
    # Live pilot 2026-10-30: a voice task stuck in awaiting_user before any run
    # blocked every new project task and could not be cancelled.
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    if await session.scalar(select(Run.id).where(Run.task_id == task.id).limit(1)) is not None:
        return False
    _check_task_version(task, expected_task_version)
    if task.status is TaskStatus.CANCELLED:
        return True
    if task.status in USER_TERMINAL_TASK_STATUSES:
        raise ValueError(f"terminal task cannot cancel: {task.status.value}")
    await transition_task(session, task, TaskStatus.CANCELLED, actor_type="user", actor_id=actor_id)
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.cancel_requested",
            actor_type="user",
            actor_id=actor_id,
            payload={"rollback_attempted": False, "runless": True},
        )
    )
    return True


async def retry_blocked_step(
    session: AsyncSession, step_id: uuid.UUID, *, actor_id: str | None = None
) -> None:
    step = await session.scalar(select(Step).where(Step.id == step_id).with_for_update())
    if step is None or step.status is not StepStatus.BLOCKED:
        raise ValueError("only a blocked step can be retried")
    explicit_retry = not step.unknown_effects and step.attempt_count >= step.max_attempts
    if not blocked_step_is_retryable(step):
        raise ValueError("blocked step is not safely retryable")
    if explicit_retry and step.attempt_count >= step.max_attempts:
        # Each explicit user retry grants exactly one additional bounded attempt.
        step.max_attempts += 1
    if step.executor_profile_id is not None and should_fallback_provider(step.failure_category):
        step.payload = {
            **step.payload,
            "retry_failed_profile_id": step.executor_profile_id,
        }
    else:
        step.payload = {
            key: value for key, value in step.payload.items() if key != "retry_failed_profile_id"
        }
    run = await session.scalar(select(Run).where(Run.id == step.run_id).with_for_update())
    assert run is not None
    task = await session.scalar(select(Task).where(Task.id == run.task_id).with_for_update())
    assert task is not None
    if actor_id is not None and step.step_type == "review":
        _reset_review_pin(session, step, actor_id=actor_id)
    task.budget_epoch += 1
    run.selected_route = {
        **run.selected_route,
        "recovery_epoch": task.budget_epoch,
        "recovery_epoch_started_at": datetime.now(UTC).isoformat(),
    }
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.budget_epoch_started",
            actor_type="user",
            actor_id=actor_id,
            payload={"budget_epoch": task.budget_epoch, "reason": "explicit_retry"},
        )
    )
    await transition_step(session, step, StepStatus.QUEUED, actor_type="user", actor_id=actor_id)
    step.failure_category = None
    step.failure_summary = None
    if run.status is RunStatus.BLOCKED:
        await transition_run(session, run, RunStatus.RUNNING, actor_type="user", actor_id=actor_id)
        run.failure_category = None
        run.failure_summary = None
    if task.status in {TaskStatus.BLOCKED, TaskStatus.QUOTA_EXHAUSTED}:
        await transition_task(
            session, task, TaskStatus.RETRYING, actor_type="user", actor_id=actor_id
        )
        await _enqueue_telegram_projection(session, task, run)


def _reset_review_pin(session: AsyncSession, step: Step, *, actor_id: str) -> None:
    """Explicit owner retry starts a fresh binding; never loosen an active lease."""
    old_pin = step.payload.get("cli_reviewer_pin")
    if old_pin is None:
        return
    session.add(
        Event(
            entity_type="step",
            entity_id=step.id,
            event_type="step.cli_reviewer_rebind_requested",
            actor_type="user",
            actor_id=actor_id,
            payload={"previous_pin": old_pin, "lease_generation": step.lease_generation},
        )
    )
    step.payload = {
        key: value
        for key, value in step.payload.items()
        if key not in {"cli_reviewer_pin", "executor_model_override", "executor_reasoning_effort"}
    }


async def recover_unsent_cli_review(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    user_id: int,
    expected_task_version: int | None,
    step_id: uuid.UUID | None,
) -> None:
    """Narrow operator escape for legacy pre-transport review fencing failures.

    Terminal transitions stay closed in the general workflow. Only the owner,
    with an exact revision and retained validated source, can grant one attempt
    here. The supervised transport persists its process BEFORE any launch; a
    process in the failed generation therefore rules out this recovery entirely.
    """
    from vuzol.providers.budgets import release_reservation_unfenced
    from vuzol.storage.models import (
        ProviderBudgetReservation,
        SupervisedProcess,
        UsageRecord,
        Worktree,
    )
    from vuzol.storage.types import BudgetReservationStatus

    task, run, steps = await _locked_context(session, task_id)
    if task.user_id != user_id:
        raise ValueError("review recovery requires the task owner's principal")
    if expected_task_version is None or expected_task_version != task.version:
        raise ValueError("stale task version for review recovery")
    target = next((step for step in steps if step.id == step_id), None)
    if (
        task.status is not TaskStatus.FAILED
        or run.status is not RunStatus.FAILED
        or target is None
        or target.step_type != "review"
        or target.status is not StepStatus.FAILED
        or target.failure_category != "handler_exception"
        or target.failure_summary != "LeaseLost"
        or target.unknown_effects
        or target.lease_owner is not None
        or target.lease_generation < 1
        or task.project_id is None
        or not target.executor_profile_id
        or not isinstance(target.payload.get("cli_reviewer_pin"), dict)
        or target.payload["cli_reviewer_pin"].get("profile_id") != target.executor_profile_id
    ):
        raise ValueError("review recovery requires one failed, pinned prelaunch CLI review")
    if any(
        step.status in {StepStatus.LEASED, StepStatus.RUNNING}
        or (step.status in {StepStatus.FAILED, StepStatus.BLOCKED} and step.id != target.id)
        for step in steps
    ):
        raise ValueError("review recovery cannot race active work or unrelated failures")
    if await session.scalar(
        select(SupervisedProcess.id)
        .where(
            SupervisedProcess.step_id == target.id,
            SupervisedProcess.lease_generation >= target.lease_generation,
        )
        .limit(1)
    ):
        raise ValueError("review recovery refuses a generation with transport evidence")
    worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
    if (
        worktree is None
        or worktree.task_id != task.id
        or worktree.project_id != task.project_id
        or worktree.delivery_state is not WorktreeDeliveryState.WORKTREE_RETAINED
        or worktree.result_commit is None
        or not any(
            s.step_type in {"execute_code", "execute_agent"} and s.status is StepStatus.COMPLETED
            for s in steps
        )
        or not any(s.step_type == "validate" and s.status is StepStatus.COMPLETED for s in steps)
        or any(
            s.attempt_count
            for s in steps
            if s.ordinal > target.ordinal
            and s.step_type
            in {
                "produce_artifacts",
                "build_static",
                "publish_preview",
                "approval",
                "apply_result",
                "publish_static",
                "finalize",
            }
        )
    ):
        raise ValueError("review recovery requires retained validated source before delivery")
    # Preserve all historical charges, including conservative orphan receipts.
    # Only still-RESERVED accounting with no receipt can be released as unsent.
    reservations = (
        await session.scalars(
            select(ProviderBudgetReservation)
            .where(
                ProviderBudgetReservation.step_id == target.id,
                ProviderBudgetReservation.budget_epoch == task.budget_epoch,
            )
            .with_for_update()
        )
    ).all()
    if not reservations or any(r.profile_id != target.executor_profile_id for r in reservations):
        raise ValueError("review recovery requires matching failed-epoch accounting")
    for reservation in reservations:
        if reservation.status is BudgetReservationStatus.RESERVED:
            if await session.scalar(
                select(SupervisedProcess.id)
                .where(
                    SupervisedProcess.step_id == target.id,
                    SupervisedProcess.provider_attempt == reservation.provider_attempt,
                )
                .limit(1)
            ):
                raise ValueError("review recovery refuses a reservation with transport evidence")
            if await session.scalar(
                select(UsageRecord.id).where(UsageRecord.reservation_id == reservation.id).limit(1)
            ):
                raise ValueError("review recovery refuses an unreconciled usage receipt")
            await release_reservation_unfenced(session, reservation_id=reservation.id)
    actor_id = str(user_id)
    old_failure = {"category": target.failure_category, "summary": target.failure_summary}
    _reset_review_pin(session, target, actor_id=actor_id)
    target.payload = {**target.payload, "review_only_recovery": True}
    target.max_attempts = target.attempt_count + 1
    task.completed_at = None
    run.ended_at = None
    task.budget_epoch += 1
    run.selected_route = {
        **run.selected_route,
        "recovery_epoch": task.budget_epoch,
        "recovery_epoch_started_at": datetime.now(UTC).isoformat(),
    }
    for entity, kind, status in (
        (target, "step", StepStatus.QUEUED),
        (run, "run", RunStatus.RUNNING),
        (task, "task", TaskStatus.RETRYING),
    ):
        previous = entity.status.value
        entity.status = status
        entity.version += 1
        session.add(
            Event(
                entity_type=kind,
                entity_id=entity.id,
                event_type=f"{kind}.status_changed",
                actor_type="operator",
                actor_id=actor_id,
                previous_state=previous,
                new_state=status.value,
                payload={"reason": "explicit_unsent_review_recovery"},
            )
        )
    target.failure_category = target.failure_summary = None
    run.failure_category = run.failure_summary = None
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.unsent_review_recovery",
            actor_type="operator",
            actor_id=actor_id,
            payload={
                "ingress_source": "cli",
                "step_id": str(target.id),
                "expected_task_version": expected_task_version,
                "previous_failure": old_failure,
                "lease_generation": target.lease_generation,
                "attempt_count": target.attempt_count,
                "max_attempts": target.max_attempts,
                "budget_epoch": task.budget_epoch,
            },
        )
    )
    await _enqueue_telegram_projection(session, task, run)


async def recover_validation_environment(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    user_id: int,
    expected_task_version: int | None,
    step_id: uuid.UUID | None,
) -> None:
    """Owner-requested environment recovery, not another source/model attempt.

    Preserve the failed validation and completed source. Retire only its mutually
    bound, never-started repair; queue the existing dependency/capability path.
    """

    from vuzol.storage.models import (
        ProviderBudgetReservation,
        SupervisedProcess,
        UsageRecord,
        Worktree,
    )
    from vuzol.storage.types import IdempotencyClass, QueueClass, RetryClass

    task, run, steps = await _locked_context(session, task_id)
    if task.user_id != user_id:
        raise ValueError("environment recovery requires the task owner's principal")
    if expected_task_version is None or task.version != expected_task_version:
        raise ValueError("stale task version for environment recovery")
    target = next((step for step in steps if step.id == step_id), None)
    if (
        run.status is not RunStatus.BLOCKED
        or target is None
        or target.step_type != "validate"
        or target.status is not StepStatus.BLOCKED
        or target.unknown_effects
        or task.project_id is None
        or target.failure_category
        not in {"validation_gate_failed", "validation_environment_missing_tool"}
        or target.payload.get("environment_recovery_step_id")
    ):
        raise ValueError("environment recovery requires an unambiguous failed validation")
    if any(step.status in {StepStatus.LEASED, StepStatus.RUNNING} for step in steps):
        raise ValueError("environment recovery cannot race active work")
    worktree = await session.scalar(
        select(Worktree).where(
            Worktree.run_id == run.id,
            Worktree.task_id == task.id,
            Worktree.project_id == task.project_id,
        )
    )
    if (
        worktree is None
        # Legacy worker finalization can record the unchanged base commit before
        # a later successful worker writes uncommitted source. It is not a new,
        # validated result; retain that evidence and prepare its environment.
        or worktree.result_commit not in {None, worktree.base_commit}
        or worktree.delivery_state
        not in {WorktreeDeliveryState.ACTIVE, WorktreeDeliveryState.WORKTREE_RETAINED}
        or not any(
            step.step_type == "execute_code" and step.status is StepStatus.COMPLETED
            for step in steps
        )
    ):
        raise ValueError("environment recovery requires retained, unfinalized completed source")
    repairs = [step for step in steps if step.status is StepStatus.BLOCKED and step.id != target.id]
    if len(repairs) > 1:
        raise ValueError("unrelated blocked steps require attention")
    if repairs:
        repair = repairs[0]
        if (
            repair.step_type != "execute_code"
            or repair.attempt_count != 0
            or repair.unknown_effects
            or repair.payload.get("repair_for_step_id") != str(target.id)
            or target.payload.get("repair_scheduled_step_id") != str(repair.id)
            or repair.dependency_metadata.get("template_key") != "repair_code"
            or repair.dependency_metadata.get("repair_for_ordinal") != target.ordinal
        ):
            raise ValueError("source repair has already started or is not mutually bound")
        for model in (SupervisedProcess, UsageRecord, ProviderBudgetReservation):
            if await session.scalar(select(model.id).where(model.step_id == repair.id).limit(1)):
                raise ValueError("source repair has execution or accounting evidence")
        await transition_step(
            session,
            repair,
            StepStatus.CANCELLED,
            actor_type="operator",
            actor_id=str(user_id),
            payload={"reason": "environment_recovery"},
        )
    preparation = Step(
        run_id=run.id,
        ordinal=max(step.ordinal for step in steps) + 1,
        step_type="ensure_dependencies",
        queue_class=QueueClass.HEAVY,
        status=StepStatus.QUEUED,
        required_capabilities=["project_shell", "filesystem_write"],
        dependency_metadata={
            "template_key": "validation_environment_recovery",
            "predecessor_ordinals": [],
        },
        payload={"validation_environment_for_step_id": str(target.id)},
        retry_class=RetryClass.NEVER,
        idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
        max_attempts=1,
        timeout_seconds=1800,
    )
    session.add(preparation)
    await session.flush()
    target.payload = {**target.payload, "environment_recovery_step_id": str(preparation.id)}
    await transition_run(
        session, run, RunStatus.RUNNING, actor_type="operator", actor_id=str(user_id)
    )
    await transition_task(
        session, task, TaskStatus.RETRYING, actor_type="operator", actor_id=str(user_id)
    )
    session.add(
        Event(
            entity_type="task",
            entity_id=task.id,
            event_type="task.validation_environment_recovery",
            actor_type="operator",
            actor_id=str(user_id),
            payload={
                "ingress_source": "cli",
                "expected_task_version": expected_task_version,
                "validation_step_id": str(target.id),
                "preparation_step_id": str(preparation.id),
            },
        )
    )
    await _enqueue_telegram_projection(session, task, run)


async def _locked_context(
    session: AsyncSession, task_id: uuid.UUID
) -> tuple[Task, Run, tuple[Step, ...]]:
    task = await session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    run = await session.scalar(
        select(Run).where(Run.task_id == task.id).order_by(Run.created_at.desc()).with_for_update()
    )
    if run is None:
        raise ValueError(f"task has no run: {task_id}")
    return task, run, await _steps(session, run.id, for_update=True)


async def _steps(
    session: AsyncSession, run_id: uuid.UUID, *, for_update: bool = False
) -> tuple[Step, ...]:
    statement = select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
    if for_update:
        statement = statement.with_for_update()
    return tuple((await session.scalars(statement)).all())


async def _noop(
    session: AsyncSession, run_id: uuid.UUID, action: str, actor_id: str | None
) -> None:
    session.add(
        Event(
            entity_type="run",
            entity_id=run_id,
            event_type="workflow.control_noop",
            actor_type="user",
            actor_id=actor_id,
            payload={"action": action},
        )
    )
    await session.flush()
