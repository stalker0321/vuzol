"""Trusted, idempotent application of an explicitly approved retained result."""

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import DeliveryMode
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.git import GitError, LocalGit
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Approval, Step, Worktree
from vuzol.storage.types import ApprovalStatus, StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash, verified_envelope


class ResultApplyHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        git: LocalGit,
    ) -> None:
        self._factory = session_factory
        self._registries = registries
        self._git = git

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(
                kind=OutcomeKind.CANCELLED,
                result={},
                category="cancelled_before_apply",
            )
        try:
            approval_id, envelope, worktree = await self._load(request)
            project = self._registries.projects.get(worktree.project_id)
            if not project.enabled:
                raise ValueError("project is disabled; approved result cannot be applied")
            if DeliveryMode.APPLY not in project.git_delivery.allowed_modes:
                raise ValueError("project policy does not allow local apply")
            if DeliveryMode.APPLY not in project.git_delivery.approval_required:
                raise ValueError("project policy does not require approval for local apply")
            if project.default_branch != envelope["target_branch"]:
                raise ValueError("project default branch changed after approval was requested")
            # Full bundle revision includes display-only profile fields (model labels,
            # planner entries). Those must not strand an already-approved local apply;
            # repository identity + delivery policy are the apply-relevant gates.
            if envelope["project_id"] != worktree.project_id:
                raise ValueError("approval project does not match retained worktree")
            identity, _remote = await self._git.repository_identity(project.repository_path)
            if identity != envelope["repository_identity_hash"]:
                raise ValueError("managed repository identity does not match the approval")
            await self._assert_current_lease(request)
            if cancellation.requested:
                return StepOutcome(
                    kind=OutcomeKind.CANCELLED,
                    result={},
                    category="cancelled_before_apply",
                )
            await self._git.apply_result(
                project.repository_path,
                Path(worktree.path),
                target_branch=envelope["target_branch"],
                expected_head=envelope["expected_target_head"],
                result_commit=envelope["result_commit"],
            )
            await self._record_applied(
                request,
                approval_id=approval_id,
                worktree_id=worktree.id,
                operation_hash=envelope_hash(envelope),
                target_branch=envelope["target_branch"],
            )
        except LeaseLost:
            cancellation.request()
            return StepOutcome(
                kind=OutcomeKind.CANCELLED,
                result={},
                category="apply_lease_lost",
            )
        except (GitError, LookupError, ValueError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="approved_result_not_applied",
                summary=str(error)[:500],
                unknown_effects=False,
            )
        return StepOutcome.succeeded({"approval_id": str(approval_id), "delivery_state": "applied"})

    async def _load(
        self, request: StepExecutionRequest
    ) -> tuple[uuid.UUID, dict[str, Any], Worktree]:
        async with self._factory() as session:
            step = await session.get(Step, request.step_id)
            if step is None:
                raise LookupError("approval step is missing")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_owner != request.lease.owner
                or step.lease_generation != request.lease.generation
                or step.run_id != request.run_id
            ):
                raise LeaseLost(f"apply step lease lost before loading: {request.step_id}")
            raw_id = step.payload.get("approval_id")
            if not isinstance(raw_id, str):
                raise ValueError("approval step has no approval identity")
            approval_id = uuid.UUID(raw_id)
            approval = await session.get(Approval, approval_id)
            if approval is None or approval.status not in {
                ApprovalStatus.APPROVED,
                ApprovalStatus.CONSUMED,
            }:
                raise ValueError("result has not been approved")
            envelope = verified_envelope(step, approval)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.run_id == request.run_id,
                    Worktree.task_id == request.task_id,
                )
            )
            if worktree is None:
                raise LookupError("approved result worktree is missing")
            expected = {
                "project_id": worktree.project_id,
                "base_commit": worktree.base_commit,
                "result_commit": worktree.result_commit,
                "diff_hash": worktree.diff_hash,
            }
            if any(envelope[key] != value for key, value in expected.items()):
                raise ValueError("retained result changed after approval was requested")
            return approval_id, envelope, worktree

    async def _record_applied(
        self,
        request: StepExecutionRequest,
        *,
        approval_id: uuid.UUID,
        worktree_id: uuid.UUID,
        operation_hash: str,
        target_branch: str,
    ) -> None:
        async with self._factory.begin() as session:
            step = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.run_id == request.run_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
                .with_for_update()
            )
            if step is None:
                raise LeaseLost(f"apply step lease lost before persistence: {request.step_id}")
            approval = await session.scalar(
                select(Approval).where(Approval.id == approval_id).with_for_update()
            )
            worktree = await session.scalar(
                select(Worktree).where(Worktree.id == worktree_id).with_for_update()
            )
            if approval is None or worktree is None:
                raise LookupError("applied result records disappeared")
            if approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
                raise ValueError("approval changed before apply was recorded")
            if worktree.delivery_state not in {
                WorktreeDeliveryState.WORKTREE_RETAINED,
                WorktreeDeliveryState.APPLIED,
            }:
                raise ValueError("worktree is not in an applicable delivery state")
            approval.status = ApprovalStatus.CONSUMED
            approval.consumed_at = approval.consumed_at or datetime.now(UTC)
            worktree.delivery_state = WorktreeDeliveryState.APPLIED
            worktree.delivery_operation_hash = operation_hash
            worktree.delivered_ref = f"refs/heads/{target_branch}"
            await session.flush()

    async def _assert_current_lease(self, request: StepExecutionRequest) -> None:
        async with self._factory() as session:
            step = await session.scalar(
                select(Step).where(
                    Step.id == request.step_id,
                    Step.run_id == request.run_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
            )
            if step is None:
                raise LeaseLost(f"apply step lease lost before side effect: {request.step_id}")
