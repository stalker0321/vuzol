"""Trusted workflow adapter for project static preview publication."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.ops.static_publish import StaticPublishError, StaticSite, publish, rollback
from vuzol.storage.models import Step, Task, Worktree
from vuzol.storage.types import StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class StaticPublishHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        runtime: RuntimeConfiguration,
        probe: Callable[[str], Awaitable[dict[str, object]]] | None = None,
    ) -> None:
        self._factory = session_factory
        self._runtime = runtime
        self._probe = probe or _probe_public_site

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
        if task is None or task.project_id is None:
            return StepOutcome.succeeded({"status": "skipped", "reason": "project_missing"})
        project = self._runtime.registries.projects.get(task.project_id)
        deployment = project.static_deployment
        if deployment is None or not deployment.enabled:
            return StepOutcome.succeeded({"status": "skipped", "reason": "not_configured"})
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
        public_url = (
            f"{str(self._runtime.settings.static_site_base_url).rstrip('/')}/{deployment.url_path}/"
        )
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
            destination=self._runtime.settings.static_site_root / deployment.url_path,
            include=deployment.include,
            entrypoint=deployment.entrypoint,
            keep_releases=deployment.keep_releases,
        )
        try:
            result = await asyncio.to_thread(
                publish,
                site,
                source_root=self._runtime.settings.worktree_root,
                site_root=self._runtime.settings.static_site_root,
            )
        except StaticPublishError as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"public_url": public_url},
                category="static_publish_failed",
                summary=str(error)[:500],
            )
        if result.release != str(build["artifact_hash"])[:20]:
            await self._rollback_changed(site, result.changed)
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"public_url": public_url, "release": result.release},
                category="static_build_hash_mismatch",
                summary="published tree does not match the approved build artifact",
            )
        try:
            probe = await self._probe(public_url)
        except (httpx.HTTPError, ValueError) as error:
            await self._rollback_changed(site, result.changed)
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"public_url": public_url, "release": result.release},
                category="static_publish_probe_failed",
                summary=str(error)[:500],
            )
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

    async def _rollback_changed(self, site: StaticSite, changed: bool) -> None:
        if changed:
            with suppress(StaticPublishError):
                await asyncio.to_thread(
                    rollback,
                    site,
                    site_root=self._runtime.settings.static_site_root,
                )


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
