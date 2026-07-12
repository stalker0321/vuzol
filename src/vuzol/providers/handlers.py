"""Step 06 handler adapter for safe model-only provider calls."""

import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import Capability
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.worktrees import WorktreeService
from vuzol.providers.budgets import account_usage, reconcile_usage, release_reservation
from vuzol.providers.domain import ProviderRequest
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.health import record_failure_observation, record_success_observation
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.routing import PROVIDER_STEP_ROLES
from vuzol.storage.models import ProviderBudgetReservation, Run, Step, Task, Worktree
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class ProviderStepHandler:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        registries: ConfigurationBundle,
        adapters: AdapterRegistry,
        *,
        worktrees: WorktreeService | None = None,
        artifacts: ArtifactStore | None = None,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._adapters = adapters
        self._worktrees = worktrees
        self._artifacts = artifacts

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        (
            provider_request,
            profile_id,
            reservation_id,
            configuration_revision,
        ) = await self._build_request(request)
        profile = self._registries.profiles.get(profile_id)
        try:
            adapter = self._adapters.get(profile_id)
            result = await adapter.execute(provider_request, profile, cancellation)
        except ProviderFailure as failure:
            async with self._factory.begin() as session:
                if failure.request_sent:
                    await reconcile_usage(
                        session,
                        reservation_id=reservation_id,
                        token=request.lease,
                        provider=profile.provider,
                        model=profile.model,
                        usage=None,
                        provider_request_id=None,
                        outcome=failure.category.value,
                        conservative=True,
                    )
                else:
                    await release_reservation(
                        session, reservation_id=reservation_id, token=request.lease
                    )
                await record_failure_observation(
                    session,
                    profile,
                    configuration_revision=configuration_revision,
                    failure=failure,
                )
            if request.step_type == "execute_code" and self._worktrees is not None:
                async with self._factory.begin() as s2:
                    wt = await s2.scalar(select(Worktree).where(Worktree.run_id == request.run_id))
                    if wt is not None:
                        await self._worktrees.retain(
                            s2,
                            worktree_id=wt.id,
                            artifacts=self._artifacts,
                            step_id=request.step_id,
                        )
            return StepOutcome(
                kind=(
                    OutcomeKind.TRANSIENT_FAILURE
                    if failure.retryable
                    else OutcomeKind.PERMANENT_FAILURE
                ),
                result={},
                category=failure.category.value,
                summary=failure.safe_summary,
                unknown_effects=False,
            )
        except (ValueError, LookupError) as error:
            async with self._factory.begin() as session:
                await release_reservation(
                    session, reservation_id=reservation_id, token=request.lease
                )
            return StepOutcome(
                kind=OutcomeKind.PERMANENT_FAILURE,
                result={},
                category="provider_configuration",
                summary=type(error).__name__,
            )
        async with self._factory.begin() as session:
            accounted_usage = account_usage(profile, result.usage)
            await reconcile_usage(
                session,
                reservation_id=reservation_id,
                token=request.lease,
                provider=profile.provider,
                model=profile.model,
                usage=accounted_usage,
                provider_request_id=result.provider_request_id,
                outcome=result.status.value,
            )
            await record_success_observation(
                session,
                profile,
                configuration_revision=configuration_revision,
            )
            if request.step_type == "execute_code" and self._worktrees is not None:
                wt = await session.scalar(select(Worktree).where(Worktree.run_id == request.run_id))
                if wt is not None:
                    await self._worktrees.retain(
                        session,
                        worktree_id=wt.id,
                        artifacts=self._artifacts,
                        step_id=request.step_id,
                    )
        return StepOutcome.succeeded(
            {
                "profile_id": profile.id,
                "model": profile.model,
                "provider_request_id": result.provider_request_id,
                "text": result.text,
                "structured_output": result.structured_output,
                "finish_reason": result.finish_reason,
            }
        )

    async def _build_request(
        self, request: StepExecutionRequest
    ) -> tuple[ProviderRequest, str, uuid.UUID, str]:
        async with self._factory() as session:
            step = await session.get(Step, request.step_id)
            run = await session.get(Run, request.run_id)
            task = await session.get(Task, request.task_id)
            if step is None or run is None or task is None or step.executor_profile_id is None:
                raise LookupError("routed provider step state is incomplete")
            reservation_value = step.payload.get("budget_reservation_id")
            attempt_value = step.payload.get("provider_attempt")
            if not isinstance(reservation_value, str) or not isinstance(attempt_value, int):
                raise LookupError("provider reservation reference is missing")
            reservation_id = uuid.UUID(reservation_value)
            reservation = await session.get(ProviderBudgetReservation, reservation_id)
            if reservation is None:
                raise LookupError("provider reservation is missing")
            profile = self._registries.profiles.get(step.executor_profile_id)
            worktree = None
            if step.step_type == "execute_code":
                worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
                if worktree is None:
                    raise LookupError("execute_code requires a prepared worktree")
            return (
                ProviderRequest(
                    task_id=task.id,
                    run_id=run.id,
                    step_id=step.id,
                    provider_attempt=attempt_value,
                    lease_generation=request.lease.generation,
                    role=PROVIDER_STEP_ROLES[step.step_type],
                    required_capabilities=frozenset(
                        Capability(value) for value in step.required_capabilities
                    ),
                    original_input_reference=f"task:{task.id}:original",
                    original_input=task.original_text,
                    task_draft=task.task_draft,
                    system_policy_revision=run.policy_revision,
                    prompt_revision=run.prompt_revision or "provider-step-v1",
                    timeout_seconds=step.timeout_seconds,
                    max_input_tokens=reservation.reserved_input_tokens,
                    max_output_tokens=reservation.reserved_output_tokens,
                    reserved_cost_units=Decimal(reservation.reserved_cost_units),
                    reserved_quota_units=Decimal(reservation.reserved_quota_units),
                    sandbox_reference=(f"worktree:{worktree.id}" if worktree is not None else None),
                ),
                profile.id,
                reservation.id,
                run.configuration_revision,
            )


SAFE_PROVIDER_STEP_TYPES = frozenset({"execute_model", "research_execute", "synthesize", "plan"})


def provider_handlers(handler: ProviderStepHandler) -> dict[str, ProviderStepHandler]:
    return {step_type: handler for step_type in SAFE_PROVIDER_STEP_TYPES}


def executor_provider_handlers(handler: ProviderStepHandler) -> dict[str, ProviderStepHandler]:
    return {"execute_code": handler}
