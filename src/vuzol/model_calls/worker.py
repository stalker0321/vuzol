"""The executor side: lease a queued call, run it on its account, settle the answer."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ProfileRegistry
from vuzol.execution.domain import ProcessEnvelope
from vuzol.execution.ports import SandboxRuntime
from vuzol.execution.proxy_service import ProxyServiceManager
from vuzol.execution.reconciliation import mark_reaped, release_reaped_account_tickets
from vuzol.execution.turn_transport import cleanup_turn_runtime, turn_runtime_lock
from vuzol.model_calls.queue import (
    ModelCallClaim,
    call_current,
    claim_call,
    current_model_call,
    finish_call,
)
from vuzol.model_calls.request import provider_request
from vuzol.observability import get_logger
from vuzol.providers.domain import ProviderResultStatus
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import ProviderAdapter
from vuzol.storage.models import ModelCall, SupervisedProcess
from vuzol.storage.types import ProcessOutcome, ProcessStatus
from vuzol.workflows.ports import CancellationContext

_LOG = get_logger(__name__)


class ModelCallWorker:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        profiles: ProfileRegistry,
        adapter: Callable[[str], ProviderAdapter],
        *,
        owner: str,
    ) -> None:
        self._factory = factory
        self._profiles = profiles
        self._adapter = adapter
        self._owner = owner

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            claim = await claim_call(session, owner=self._owner)
        if claim is None:
            return False
        try:
            output, error, usage = await self._run(claim)
        except Exception as failure:  # an executor lane must keep running
            _LOG.warning(
                "model call failed",
                extra={"event": "model_call.failed", "error": type(failure).__name__},
                exc_info=True,
            )
            output, error, usage = None, f"model_call_{type(failure).__name__}", {}
        async with self._factory.begin() as session:
            await finish_call(session, claim, output=output, error=error, usage=usage)
        return True

    async def _run(
        self, claim: ModelCallClaim
    ) -> tuple[dict[str, Any] | None, str | None, dict[str, Any]]:
        async with self._factory() as session:
            call = await session.get(ModelCall, claim.call_id)
            assert call is not None
            request = provider_request(call, claim)
            profile = self._profiles.get(call.profile_id)
        token = current_model_call.set(claim)
        try:
            result = await self._adapter(profile.id).execute(
                request, profile, CancellationContext()
            )
        except ProviderFailure as failure:
            return None, failure.category.value, {}
        finally:
            current_model_call.reset(token)
        usage = result.usage.model_dump(mode="json")
        usage.update(profile_id=profile.id, model=profile.model)
        if result.status is ProviderResultStatus.SUCCEEDED and result.structured_output:
            return result.structured_output, None, usage
        category = result.failure.category.value if result.failure else result.status.value
        return None, category, usage


class ModelCallReconciler:
    """Reap a model call's sandbox once no live launcher holds it and its lease is gone."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        runtime: SandboxRuntime,
        proxy: ProxyServiceManager | None,
    ) -> None:
        self._factory = factory
        self._runtime = runtime
        self._proxy = proxy
        self._ticket_cursor: uuid.UUID | None = None

    async def reconcile(self) -> int:
        async with self._factory() as session:
            rows = (
                await session.execute(
                    select(SupervisedProcess.id, SupervisedProcess.invocation_id)
                    .where(
                        SupervisedProcess.model_call_id.is_not(None),
                        SupervisedProcess.reaped_at.is_(None),
                    )
                    .order_by(SupervisedProcess.created_at)
                    .limit(100)
                )
            ).all()
        reaped = 0
        for process_id, invocation_id in rows:
            async with turn_runtime_lock(self._factory, invocation_id) as connection:
                if connection is not None and await self._reap(process_id):
                    reaped += 1
        self._ticket_cursor = await release_reaped_account_tickets(
            self._factory, self._ticket_cursor
        )
        return reaped

    async def _reap(self, process_id: uuid.UUID) -> bool:
        async with self._factory.begin() as session:
            process = await session.get(SupervisedProcess, process_id, with_for_update=True)
            if process is None or process.reaped_at is not None:
                return False
            try:
                bound = process.runtime_metadata["model_call"]
                claim = ModelCallClaim(
                    uuid.UUID(bound["call_id"]), bound["owner"], int(bound["generation"])
                )
                envelope = ProcessEnvelope.model_validate(
                    {**process.runtime_metadata["runtime_envelope"], "stdin": ""}
                )
            except (KeyError, ValueError, TypeError):
                return False
            if claim.call_id != process.model_call_id or await call_current(session, claim):
                return False
            try:
                await cleanup_turn_runtime(self._runtime, self._proxy, envelope)
            except Exception:
                process.status = ProcessStatus.UNKNOWN
                process.outcome = process.outcome or ProcessOutcome.UNKNOWN
                process.runtime_metadata = {**process.runtime_metadata, "cleanup_confirmed": False}
                return False
            mark_reaped(process)
            return True
