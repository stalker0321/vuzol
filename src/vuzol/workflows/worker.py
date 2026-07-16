"""Bounded fenced workflow worker."""

import asyncio
import uuid
from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import Capability, Settings
from vuzol.config.registries import ConfigurationBundle
from vuzol.providers.routing import claim_routed_step
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_step, heartbeat_step, start_step
from vuzol.storage.models import Run, Step
from vuzol.storage.records import LeaseToken
from vuzol.storage.types import QueueClass
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest, StepHandler
from vuzol.workflows.service import commit_step_outcome


class WorkflowWorker:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        handlers: Mapping[str, StepHandler],
        capabilities: frozenset[Capability] = frozenset(Capability),
        queue_classes: frozenset[QueueClass] = frozenset(QueueClass),
        profile_limits: Mapping[str, int] | None = None,
    ) -> None:
        self._settings = settings
        self._factory = session_factory
        self._owner = owner
        self._handlers = dict(handlers)
        self._capabilities = capabilities
        self._queue_classes = queue_classes
        self._profile_limits = dict(profile_limits or {})

    async def process_one(self) -> bool:
        limits = self._settings.concurrency
        class_limits = {
            QueueClass.CONTROL: limits.control,
            QueueClass.LIGHT: limits.light,
            QueueClass.HEAVY: limits.heavy,
            QueueClass.PRIVILEGED: limits.privileged,
        }
        async with self._factory.begin() as session:
            token = await self._claim(session, class_limits)
        if token is None:
            return False
        async with self._factory.begin() as session:
            await start_step(session, token)
        request = await self._request(token.step.id, token)
        cancellation = CancellationContext()
        heartbeat = asyncio.create_task(self._heartbeat(token, cancellation))
        commit_after_cancellation = False
        try:
            handler = self._handlers[request.step_type]
            outcome = await asyncio.wait_for(
                handler.execute(request, cancellation), timeout=request.timeout_seconds
            )
        except TimeoutError:
            outcome = StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="timeout",
                summary="handler exceeded its persisted timeout",
                unknown_effects=True,
            )
        except asyncio.CancelledError:
            cancellation.request()
            commit_after_cancellation = True
            outcome = StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="shutdown_cancelled",
                summary="handler did not drain before shutdown",
                unknown_effects=True,
            )
        except Exception as error:
            outcome = StepOutcome(
                kind=OutcomeKind.PERMANENT_FAILURE,
                result={},
                category="handler_exception",
                summary=type(error).__name__,
            )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if cancellation.requested and not commit_after_cancellation:
            return True
        async with self._factory.begin() as session:
            await commit_step_outcome(
                session,
                token,
                outcome,
                retry_delay_seconds=self._retry_delay(token.step.id, token.step.lease_generation),
            )
        return True

    async def _claim(
        self, session: AsyncSession, class_limits: Mapping[QueueClass, int]
    ) -> LeaseToken | None:
        return await claim_step(
            session,
            owner=self._owner,
            lease_seconds=self._settings.workflow.lease_seconds,
            capabilities=frozenset(value.value for value in self._capabilities),
            queue_classes=self._queue_classes,
            class_limits=dict(class_limits),
            profile_limits=self._profile_limits,
            step_types=frozenset(self._handlers),
            candidate_limit=self._settings.workflow.claim_candidate_limit,
        )

    async def _request(self, step_id: uuid.UUID, token: LeaseToken) -> StepExecutionRequest:
        async with self._factory() as session:
            step = await session.get(Step, step_id)
            assert step is not None
            run = await session.get(Run, step.run_id)
            assert run is not None
            return StepExecutionRequest(
                task_id=run.task_id,
                run_id=run.id,
                step_id=step.id,
                step_type=step.step_type,
                payload=dict(step.payload),
                timeout_seconds=step.timeout_seconds,
                lease=token,
            )

    async def _heartbeat(self, token: LeaseToken, cancellation: CancellationContext) -> None:
        try:
            while True:
                await asyncio.sleep(self._settings.workflow.heartbeat_seconds)
                async with self._factory.begin() as session:
                    await heartbeat_step(
                        session, token, lease_seconds=self._settings.workflow.lease_seconds
                    )
        except LeaseLost:
            cancellation.request()
            raise

    def _retry_delay(self, step_id: uuid.UUID, generation: int) -> float:
        settings = self._settings.workflow
        base = float(
            min(settings.retry_max_seconds, settings.retry_min_seconds * 2 ** (generation - 1))
        )
        jitter = (step_id.int % 1_001) / 10_000
        return min(settings.retry_max_seconds, base * (1 + jitter))


class CompleteHandler:
    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        del request, cancellation
        return StepOutcome.succeeded()


INTERNAL_HANDLERS: dict[str, StepHandler] = {
    "await_apply_or_complete": CompleteHandler(),
    "complete_or_block": CompleteHandler(),
    "format_result": CompleteHandler(),
    "finalize": CompleteHandler(),
    # Repository inspection remains the executor agent's responsibility. This bounded step
    # records the workflow boundary without duplicating an agent-scale repository read.
    "prepare_context": CompleteHandler(),
}


class RoutedWorkflowWorker(WorkflowWorker):
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        registries: ConfigurationBundle,
        owner: str,
        handlers: Mapping[str, StepHandler],
        queue_classes: frozenset[QueueClass] | None = None,
    ) -> None:
        effective_queues = queue_classes or frozenset({QueueClass.LIGHT})
        super().__init__(
            settings,
            session_factory,
            owner=owner,
            handlers=handlers,
            queue_classes=effective_queues,
        )
        self._registries = registries

    async def _claim(
        self, session: AsyncSession, class_limits: Mapping[QueueClass, int]
    ) -> LeaseToken | None:
        return await claim_routed_step(
            session,
            settings=self._settings,
            registries=self._registries,
            owner=self._owner,
            lease_seconds=self._settings.workflow.lease_seconds,
            candidate_limit=self._settings.workflow.claim_candidate_limit,
            class_limits=dict(class_limits),
            step_types=frozenset(self._handlers),
        )
