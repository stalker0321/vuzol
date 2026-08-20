"""Hash-bound approval lifecycle for immutable project dependency environments."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import DependencyProvisioningSettings
from vuzol.execution.dependency_build import DependencyBuildError
from vuzol.execution.paths import PathViolation, contained, trusted_root
from vuzol.projects.dependencies import (
    DEPENDENCY_APPROVAL_SCHEMA,
    DependencyEnvironment,
    DependencyError,
    DependencyRequest,
    inspect_dependency_requests,
    load_dependency_environment,
)
from vuzol.projects.source_catalog import SourceCatalog
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Approval, Step, Task, Worktree
from vuzol.storage.types import ApprovalStatus, StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash, verified_envelope

DEPENDENCY_APPROVAL_TTL = timedelta(days=7)


class DependencyBuilder(Protocol):
    async def build(
        self,
        step_request: StepExecutionRequest,
        *,
        project_id: str,
        worktree_id: uuid.UUID,
        worktree: Path,
        request: DependencyRequest,
        cancellation: CancellationContext,
    ) -> DependencyEnvironment: ...


class DependencyProvisioningHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: DependencyProvisioningSettings,
        builder: DependencyBuilder | None,
        *,
        worktree_root: Path,
        catalog: SourceCatalog | None = None,
    ) -> None:
        self._factory = factory
        self._settings = settings
        self._builder = builder
        self._worktree_root = trusted_root(worktree_root, create=False)
        self._catalog = catalog or SourceCatalog.builtin()

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        side_effect_started = False
        try:
            task, step, worktree, approval = await self._load(request)
            if task.project_id is None:
                raise DependencyError("dependency task has no project")
            root = contained(self._worktree_root, Path(worktree.path))
            requirements = inspect_dependency_requests(
                root,
                self._catalog,
                maximum_direct_dependencies=self._settings.maximum_direct_dependencies,
            )
            if not requirements:
                return StepOutcome.succeeded({"status": "skipped", "environments": []})
            if not self._settings.enabled:
                return StepOutcome(
                    kind=OutcomeKind.BLOCKED,
                    result={"status": "needs_setup"},
                    category="dependency_provisioning_disabled",
                    summary="Dependency environment provisioning is disabled",
                )
            missing = tuple(
                item
                for item in requirements
                if load_dependency_environment(
                    self._settings.environment_root, task.project_id, item
                )
                is None
            )
            if not missing:
                if approval is not None and approval.status is ApprovalStatus.APPROVED:
                    await self._consume(request, approval.id)
                return StepOutcome.succeeded(
                    {
                        "status": "ready",
                        "environments": [item.environment_key for item in requirements],
                    }
                )
            if approval is None:
                approval = await self._request_approval(
                    request,
                    task=task,
                    step=step,
                    worktree=worktree,
                    requirements=missing,
                )
                return _needs_approval(approval.id, missing)
            if approval.status is ApprovalStatus.PENDING:
                return _needs_approval(approval.id, missing)
            if approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
                raise DependencyError("dependency installation was not approved")
            envelope = verified_envelope(step, approval)
            _verify_envelope(
                envelope,
                task_id=task.id,
                run_id=request.run_id,
                step_id=step.id,
                project_id=task.project_id,
                worktree_id=worktree.id,
                environment_root=self._settings.environment_root,
                requirements=missing,
            )
            environments: list[str] = []
            if self._builder is None:
                raise DependencyError("controlled dependency builder is unavailable")
            for item in missing:
                await self._assert_current_lease(request)
                if cancellation.requested:
                    return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
                side_effect_started = True
                environment = await self._builder.build(
                    request,
                    project_id=task.project_id,
                    worktree_id=worktree.id,
                    worktree=root,
                    request=item,
                    cancellation=cancellation,
                )
                environments.append(environment.request.environment_key)
            await self._consume(request, approval.id)
            return StepOutcome.succeeded(
                {
                    "status": "installed",
                    "environments": environments,
                    "approval_id": str(approval.id),
                }
            )
        except LeaseLost:
            cancellation.request()
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="lease_lost")
        except (
            DependencyBuildError,
            DependencyError,
            LookupError,
            PathViolation,
            ValueError,
        ) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="dependency_provisioning_failed",
                summary=str(error)[:500],
                unknown_effects=side_effect_started,
            )

    async def _load(
        self, request: StepExecutionRequest
    ) -> tuple[Task, Step, Worktree, Approval | None]:
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
            step = await session.get(Step, request.step_id)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.task_id == request.task_id,
                    Worktree.run_id == request.run_id,
                )
            )
            approval = await session.scalar(
                select(Approval)
                .where(Approval.step_id == request.step_id)
                .order_by(Approval.requested_at.desc())
                .limit(1)
            )
        if task is None or step is None or worktree is None:
            raise LookupError("dependency provisioning state is incomplete")
        if (
            step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
            or step.lease_owner != request.lease.owner
            or step.lease_generation != request.lease.generation
            or step.run_id != request.run_id
        ):
            raise LeaseLost("dependency provisioning step lease is stale")
        return task, step, worktree, approval

    async def _request_approval(
        self,
        request: StepExecutionRequest,
        *,
        task: Task,
        step: Step,
        worktree: Worktree,
        requirements: tuple[DependencyRequest, ...],
    ) -> Approval:
        envelope: dict[str, Any] = {
            "schema_version": DEPENDENCY_APPROVAL_SCHEMA,
            "requested_action": "install_dependencies",
            "task_id": str(task.id),
            "run_id": str(request.run_id),
            "step_id": str(step.id),
            "project_id": task.project_id,
            "worktree_id": str(worktree.id),
            "environment_root": str(self._settings.environment_root),
            "requirements": [item.approval_record() for item in requirements],
        }
        digest = envelope_hash(envelope)
        approval_id = uuid.uuid4()
        approval = Approval(
            id=approval_id,
            step_id=step.id,
            action_envelope_hash=digest,
            requested_action="install_dependencies",
            normalized_target=f"{task.project_id}:dependency-environments",
            human_summary=(
                "Собрать изолированные зависимости проекта: "
                + ", ".join(item.ecosystem for item in requirements)
            ),
            token_hash=hashlib.sha256(f"{approval_id}:{digest}".encode()).hexdigest(),
            status=ApprovalStatus.PENDING,
            expires_at=datetime.now(UTC) + DEPENDENCY_APPROVAL_TTL,
        )
        async with self._factory.begin() as session:
            locked = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
                .with_for_update()
            )
            if locked is None:
                raise LeaseLost("dependency approval lost its step lease")
            existing = await session.scalar(select(Approval).where(Approval.step_id == step.id))
            if existing is not None:
                return existing
            session.add(approval)
            locked.payload = {
                **locked.payload,
                "approval_id": str(approval.id),
                "action_envelope": envelope,
            }
            locked.external_idempotency_key = f"install-dependencies:{digest}"
            await session.flush()
        return approval

    async def _assert_current_lease(self, request: StepExecutionRequest) -> None:
        async with self._factory() as session:
            step = await session.scalar(
                select(Step).where(
                    Step.id == request.step_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
            )
            if step is None:
                raise LeaseLost("dependency provisioning lease was lost before installation")

    async def _consume(self, request: StepExecutionRequest, approval_id: uuid.UUID) -> None:
        async with self._factory.begin() as session:
            step = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                )
                .with_for_update()
            )
            approval = await session.scalar(
                select(Approval).where(Approval.id == approval_id).with_for_update()
            )
            if step is None or approval is None:
                raise LeaseLost("dependency provisioning records disappeared")
            if approval.status not in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
                raise DependencyError("dependency approval changed before completion")
            approval.status = ApprovalStatus.CONSUMED
            approval.consumed_at = approval.consumed_at or datetime.now(UTC)


def _needs_approval(
    approval_id: uuid.UUID, requirements: tuple[DependencyRequest, ...]
) -> StepOutcome:
    return StepOutcome(
        kind=OutcomeKind.NEEDS_APPROVAL,
        result={
            "approval_id": str(approval_id),
            "ecosystems": [item.ecosystem for item in requirements],
        },
        category="dependency_installation_approval_required",
        summary="Зависимости проекта требуют отдельного разрешения.",
    )


def _verify_envelope(
    envelope: dict[str, Any],
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    project_id: str,
    worktree_id: uuid.UUID,
    environment_root: Path,
    requirements: tuple[DependencyRequest, ...],
) -> None:
    if (
        envelope.get("schema_version") != DEPENDENCY_APPROVAL_SCHEMA
        or envelope.get("requested_action") != "install_dependencies"
        or envelope.get("task_id") != str(task_id)
        or envelope.get("run_id") != str(run_id)
        or envelope.get("step_id") != str(step_id)
        or envelope.get("project_id") != project_id
        or envelope.get("worktree_id") != str(worktree_id)
        or envelope.get("environment_root") != str(environment_root)
        or envelope.get("requirements") != [item.approval_record() for item in requirements]
    ):
        raise DependencyError("dependency approval no longer matches the current task")
