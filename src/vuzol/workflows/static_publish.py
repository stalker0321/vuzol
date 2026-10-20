"""Trusted workflow adapter for project static preview publication."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.ops.static_publish import (
    PublishResult,
    StaticHashMismatch,
    StaticPublishError,
    StaticSite,
    activate,
    compensate,
    prepare,
)
from vuzol.storage.models import (
    MaterializationLink,
    PlanRevisionItem,
    Step,
    Task,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.publication import (
    PublicationDenied,
    ensure_production_approval,
    publication_authority,
)


class StaticPublishHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        runtime: RuntimeConfiguration,
        probe: Callable[[str], Awaitable[dict[str, object]]] | None = None,
        preview: bool = False,
    ) -> None:
        self._factory = session_factory
        self._runtime = runtime
        self._probe = probe or _probe_public_site
        self._preview = preview

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.task_id == request.task_id,
                    Worktree.run_id == request.run_id,
                )
            )
            build_step = await session.scalar(
                select(Step).where(
                    Step.run_id == request.run_id,
                    Step.step_type == "build_static",
                    Step.status == StepStatus.COMPLETED,
                )
            )
            materialization = await session.scalar(
                select(MaterializationLink).where(MaterializationLink.task_id == request.task_id)
            )
            plan_size = None
            managed_goal_id = None
            if materialization is not None:
                managed_goal_id = await session.scalar(
                    select(WorkPackage.managed_goal_id).where(
                        WorkPackage.id == materialization.work_package_id
                    )
                )
                plan_size = await session.scalar(
                    select(func.count(PlanRevisionItem.id)).where(
                        PlanRevisionItem.plan_revision_id == materialization.plan_revision_id
                    )
                )
        if task is None or task.project_id is None:
            return StepOutcome.succeeded({"status": "skipped", "reason": "project_missing"})
        project = self._runtime.registries.projects.get(task.project_id)
        deployment = project.static_deployment
        if deployment is None or not deployment.enabled:
            return StepOutcome.succeeded({"status": "skipped", "reason": "not_configured"})
        if not self._preview and managed_goal_id is not None:
            return StepOutcome.succeeded(
                {"status": "skipped", "reason": "managed_integration_only"}
            )
        if (
            not self._preview
            and materialization is not None
            and plan_size is not None
            and materialization.ordinal < int(plan_size)
        ):
            return StepOutcome.succeeded(
                {
                    "status": "skipped",
                    "reason": "package_intermediate_item",
                    "ordinal": materialization.ordinal,
                    "plan_size": int(plan_size),
                }
            )
        build = (
            build_step.result
            if build_step is not None and isinstance(build_step.result, dict)
            else {}
        )
        if (
            worktree is None
            or build.get("status") != "built"
            or build.get("source_commit") != worktree.result_commit
            or build.get("source_directory") != deployment.source_directory.as_posix()
            or not isinstance(build.get("artifact_hash"), str)
        ):
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="static_build_evidence_missing",
                summary="publication requires a build artifact bound to the retained result",
            )
        source = Path(worktree.path) / deployment.source_directory
        entrypoint = source / deployment.entrypoint
        site_root = (
            self._runtime.settings.preview_site_root
            if self._preview
            else self._runtime.settings.static_site_root
        )
        site_base_url = (
            self._runtime.settings.preview_site_base_url
            if self._preview
            else self._runtime.settings.static_site_base_url
        )
        public_url = f"{str(site_base_url).rstrip('/')}/{deployment.url_path}/"
        if not entrypoint.is_file() or entrypoint.is_symlink():
            return StepOutcome.succeeded(
                {
                    "status": "skipped",
                    "reason": "entrypoint_missing",
                    "public_url": public_url,
                }
            )
        site = StaticSite(
            id=project.id,
            source=source,
            destination=site_root / deployment.url_path,
            include=deployment.include,
            entrypoint=deployment.entrypoint,
            keep_releases=deployment.keep_releases,
        )
        production_target = (
            None
            if self._preview
            else {
                "source_commit": build["source_commit"],
                "artifact_hash": build["artifact_hash"],
                "destination": str(site.destination.resolve()),
                "public_url": public_url,
            }
        )
        try:
            if production_target is not None and not await self._ensure_approval(
                request, cancellation, production_target
            ):
                return StepOutcome(
                    kind=OutcomeKind.NEEDS_APPROVAL,
                    result={"public_url": public_url},
                    category="production_approval_required",
                )
            prepared = await asyncio.to_thread(
                prepare,
                site,
                source_root=self._runtime.settings.worktree_root,
                site_root=site_root,
                expected_hash=str(build["artifact_hash"]),
            )
            if cancellation.requested:
                raise PublicationDenied("cancelled")
            async with self._authority(request, cancellation, production_target=production_target):
                # No await between the final grant check and the atomic swap.
                result = activate(prepared)
        except PublicationDenied as error:
            return StepOutcome(
                kind=OutcomeKind.CANCELLED if cancellation.requested else OutcomeKind.BLOCKED,
                result={},
                category=str(error),
            )
        except StaticPublishError as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"public_url": public_url},
                category=(
                    "static_build_hash_mismatch"
                    if isinstance(error, StaticHashMismatch)
                    else "static_publish_failed"
                ),
                summary=str(error)[:500],
            )
        try:
            probe = await self._probe(public_url)
        except asyncio.CancelledError:
            self._rollback_changed(site, result, site_root=site_root)
            raise
        except (httpx.HTTPError, ValueError) as error:
            try:
                self._rollback_changed(site, result, site_root=site_root)
            except (StaticPublishError, OSError) as rollback_error:
                return StepOutcome(
                    kind=OutcomeKind.BLOCKED,
                    result={"public_url": public_url},
                    category="static_publish_compensation_failed",
                    summary=str(rollback_error)[:500],
                    unknown_effects=True,
                )
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"public_url": public_url, "release": result.release},
                category="static_publish_probe_failed",
                summary=str(error)[:500],
            )
        if self._preview and materialization is not None:
            async with self._factory.begin() as session:
                package = await session.get(WorkPackage, materialization.work_package_id)
                if package is not None:
                    package.preview_url = public_url
        return StepOutcome.succeeded(
            {
                "status": "published",
                "public_url": public_url,
                "release": result.release,
                "changed": result.changed,
                "files": result.files,
                "bytes": result.bytes,
                "probe": probe,
                "source_commit": build["source_commit"],
                "artifact_hash": build["artifact_hash"],
            }
        )

    def _authority(
        self,
        request: StepExecutionRequest,
        cancellation: CancellationContext,
        *,
        production_target: dict[str, Any] | None = None,
    ) -> AbstractAsyncContextManager[None]:
        return publication_authority(
            self._factory,
            request,
            cancellation,
            feature_enabled=self._runtime.settings.project_lead.enabled,
            production_target=production_target,
        )

    async def _ensure_approval(
        self,
        request: StepExecutionRequest,
        cancellation: CancellationContext,
        target: dict[str, Any],
    ) -> bool:
        return await ensure_production_approval(self._factory, request, cancellation, target)

    def _rollback_changed(
        self, site: StaticSite, result: PublishResult, *, site_root: Path
    ) -> None:
        compensate(site, result, site_root=site_root)


async def _probe_public_site(public_url: str) -> dict[str, object]:
    async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
        response = await client.get(public_url, headers={"accept": "text/html"})
    if response.status_code != 200:
        raise ValueError(f"published site returned HTTP {response.status_code}")
    if not response.content.strip():
        raise ValueError("published site returned an empty response")
    return {
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type", "")[:100],
        "bytes": len(response.content),
        "final_url": str(response.url),
    }
