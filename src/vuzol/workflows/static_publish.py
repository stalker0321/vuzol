"""Trusted workflow adapter for project static preview publication."""

from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.ops.static_publish import StaticPublishError, StaticSite, publish
from vuzol.storage.models import Task
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class StaticPublishHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        runtime: RuntimeConfiguration,
    ) -> None:
        self._factory = session_factory
        self._runtime = runtime

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
        if task is None or task.project_id is None:
            return StepOutcome.succeeded({"status": "skipped", "reason": "project_missing"})
        project = self._runtime.registries.projects.get(task.project_id)
        deployment = project.static_deployment
        if deployment is None or not deployment.enabled:
            return StepOutcome.succeeded({"status": "skipped", "reason": "not_configured"})
        source = project.repository_path / deployment.source_directory
        entrypoint = source / deployment.entrypoint
        public_url = (
            f"{str(self._runtime.settings.static_site_base_url).rstrip('/')}"
            f"/{deployment.url_path}/"
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
                source_root=self._runtime.settings.repository_root,
                site_root=self._runtime.settings.static_site_root,
            )
        except StaticPublishError as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"public_url": public_url},
                category="static_publish_failed",
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
            }
        )
