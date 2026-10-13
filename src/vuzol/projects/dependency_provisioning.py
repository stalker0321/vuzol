"""Automatic, hash-bound preparation of isolated project dependencies.

Legacy approval records remain readable, but ordinary dependency preparation
does not create a new human-approval boundary.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import DependencyProvisioningSettings
from vuzol.execution.dependency_build import DependencyBuildError
from vuzol.execution.paths import PathViolation, contained, trusted_root
from vuzol.projects.capability_provisioning import CapabilityProvisioningHandler
from vuzol.projects.custom_sources import CustomDependencySource
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
from vuzol.storage.models import Approval, ProjectDependencySource, Step, Task, Worktree
from vuzol.storage.types import ApprovalStatus, StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import verified_envelope


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
        capabilities: CapabilityProvisioningHandler | None = None,
    ) -> None:
        self._factory = factory
        self._settings = settings
        self._builder = builder
        self._worktree_root = trusted_root(worktree_root, create=False)
        self._catalog = catalog or SourceCatalog.builtin()
        self._capabilities = capabilities

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        side_effect_started = False
        try:
            task, step, worktree, approval, custom_sources = await self._load(request)
            if task.project_id is None:
                raise DependencyError("dependency task has no project")
            root = contained(self._worktree_root, Path(worktree.path))
            if self._capabilities is not None:
                capability_outcome = await self._capabilities.ensure_before_dependencies(
                    request, cancellation
                )
                if capability_outcome.kind is not OutcomeKind.SUCCEEDED:
                    return capability_outcome
            requirements = inspect_dependency_requests(
                root,
                self._catalog,
                maximum_direct_dependencies=self._settings.maximum_direct_dependencies,
                custom_sources=custom_sources,
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
            # Keep approved legacy envelopes hash-bound, and respect an explicit
            # rejection. Pending legacy records are not fabricated as approved.
            if approval is not None:
                if approval.status not in {
                    ApprovalStatus.APPROVED,
                    ApprovalStatus.CONSUMED,
                    ApprovalStatus.PENDING,
                }:
                    raise DependencyError("legacy dependency installation was rejected or expired")
                if approval.status in {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}:
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
            if approval is not None and approval.status is ApprovalStatus.APPROVED:
                await self._consume(request, approval.id)
            return StepOutcome.succeeded(
                {
                    "status": "installed",
                    "environments": environments,
                    "authorization": "project_local_dependency_policy",
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
    ) -> tuple[
        Task,
        Step,
        Worktree,
        Approval | None,
        tuple[CustomDependencySource, ...],
    ]:
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
                .where(
                    Approval.step_id == request.step_id,
                    Approval.requested_action == "install_dependencies",
                )
                .order_by(Approval.requested_at.desc())
                .limit(1)
            )
            source_rows = (
                ()
                if task is None or task.project_id is None
                else tuple(
                    (
                        await session.scalars(
                            select(ProjectDependencySource).where(
                                ProjectDependencySource.project_id == task.project_id,
                                ProjectDependencySource.revoked_at.is_(None),
                            )
                        )
                    ).all()
                )
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
        sources = tuple(
            CustomDependencySource(
                id=row.id,
                project_id=row.project_id,
                ecosystem=row.ecosystem,
                package_name=row.package_name,
                source_kind=row.source_kind,
                source_url=row.source_url,
                source_pin=row.source_pin,
            )
            for row in source_rows
        )
        return task, step, worktree, approval, sources

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
