"""Concurrent research attempts, authorized and renewed by job leases alone."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.execution.git import GitError
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import (
    TurnInvocationContext,
    current_turn,
    invocation_current,
)
from vuzol.leadership.jobs import JobClaim, claim_jobs, complete_job
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.persistence import LeadershipTurn, LeadJob, ManagedGoal
from vuzol.storage.errors import LeaseLost


class ResearchModel(Protocol):
    async def research(self, job: LeadJob) -> tuple[str, dict[str, Any]]: ...


class ResearchJobRunner:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        model: ResearchModel,
        enabled: Callable[[], bool],
        concurrency: int = 2,
        lease_seconds: int = 120,
    ) -> None:
        self._factory, self._owner, self._model = factory, owner, model
        self._enabled, self._concurrency, self._lease_seconds = enabled, concurrency, lease_seconds
        self._attempts: set[asyncio.Task[None]] = set()
        self._loop: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._loop is None:
            self._loop = asyncio.create_task(self.run(), name="lead-research-jobs")
        elif self._loop.done():
            self._loop.result()

    async def run(self) -> None:
        try:
            while True:
                await self.tick()
                await asyncio.sleep(min(1.0, self._lease_seconds / 3))
        finally:
            for task in self._attempts:
                task.cancel()
            await asyncio.gather(*self._attempts, return_exceptions=True)

    async def aclose(self) -> None:
        if self._loop is not None:
            self._loop.cancel()
            with suppress(asyncio.CancelledError):
                await self._loop
        else:
            for task in self._attempts:
                task.cancel()
            await asyncio.gather(*self._attempts, return_exceptions=True)

    async def wait_idle(self) -> None:
        await asyncio.gather(*self._attempts)

    async def tick(self) -> bool:
        for task in list(self._attempts):
            if task.done():
                self._attempts.remove(task)
                task.result()
        if not self._enabled():
            return False
        async with self._factory.begin() as session:
            claims = await claim_jobs(
                session,
                owner=self._owner,
                concurrency=self._concurrency,
                lease_seconds=self._lease_seconds,
            )
        for claim in claims:
            self._attempts.add(asyncio.create_task(self._attempt(claim)))
        return bool(claims)

    async def _authority(self, claim: JobClaim) -> TurnInvocationContext:
        async with self._factory.begin() as session:
            goal = await session.get(ManagedGoal, claim.goal_id, with_for_update=True)
            if goal is None:
                raise LeaseLost("research goal missing")
            turn_id = uuid.uuid5(claim.job_id, f"attempt:{claim.generation}")
            session.add(
                LeadershipTurn(
                    id=turn_id,
                    goal_id=goal.id,
                    wakeup_id=turn_id,
                    lease_epoch=claim.generation,
                    goal_version=goal.version,
                    scope_revision=goal.scope_revision,
                    fence_epoch=goal.fence_epoch,
                    status="claimed",
                    owner=claim.owner,
                )
            )
            await session.flush()
            return TurnInvocationContext(
                claim=TurnClaim(
                    goal.id, claim.job_id, claim.owner, claim.generation, claim.generation
                ),
                turn_id=turn_id,
                project_id=goal.project_id,
                version=goal.version,
                fence_epoch=goal.fence_epoch,
                scope_revision=goal.scope_revision,
                job=True,
                enabled=self._enabled,
            )

    async def _renew(self, context: TurnInvocationContext) -> None:
        while True:
            await asyncio.sleep(self._lease_seconds / 3)
            async with asyncio.timeout(5), self._factory.begin() as session:
                if not await invocation_current(session, context):
                    raise LeaseLost("research lease lost")
                job = await session.get(LeadJob, context.claim.wakeup_id)
                assert job is not None
                job.lease_expires_at = datetime.now(UTC) + timedelta(seconds=self._lease_seconds)

    async def _attempt(self, claim: JobClaim) -> None:
        context = await self._authority(claim)
        binding = current_turn.set(context)
        renewal = asyncio.create_task(self._renew(context))
        call = asyncio.create_task(self._research(claim))
        applied = False
        try:
            done, _ = await asyncio.wait((call, renewal), return_when=asyncio.FIRST_COMPLETED)
            if renewal in done:
                # A failed renewal (including a database outage) revokes application authority.
                with suppress(Exception):
                    renewal.result()
                return
            report, reason, usage = await call
            async with self._factory.begin() as session:
                if await invocation_current(session, context):
                    applied = await complete_job(
                        session, claim, result_text=report, failure_reason=reason, usage=usage
                    )
        finally:
            for task in (call, renewal):
                if not task.done():
                    task.cancel()
            await asyncio.gather(call, renewal, return_exceptions=True)
            current_turn.reset(binding)
            async with self._factory.begin() as session:
                turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=True)
                assert turn is not None
                turn.status = "applied" if applied else "lost"

    async def _research(self, claim: JobClaim) -> tuple[str | None, str | None, dict[str, Any]]:
        try:
            async with self._factory() as session:
                job = await session.get(LeadJob, claim.job_id)
                assert job is not None
            report, usage = await self._model.research(job)
            return report, None, usage
        except LeadModelUnavailable as error:
            return None, str(error), error.usage
        except (ValueError, GitError, OSError):
            return None, "invalid_output_or_repository_unavailable", {}
