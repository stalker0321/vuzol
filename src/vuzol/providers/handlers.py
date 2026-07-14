"""Step 06 handler adapter for safe model-only provider calls."""

import uuid
from decimal import Decimal

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import Capability
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.finalization import (
    FinalizedWorkerResult,
    GateExecutionContext,
    WorkerFinalizationError,
    WorkerFinalizer,
)
from vuzol.execution.worktrees import WorktreeService
from vuzol.providers.budgets import account_usage, reconcile_usage, release_reservation
from vuzol.providers.domain import ProviderRequest, ProviderResult
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
        finalizer: WorkerFinalizer | None = None,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._adapters = adapters
        self._worktrees = worktrees
        self._artifacts = artifacts
        self._finalizer = finalizer

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        (
            provider_request,
            profile_id,
            reservation_id,
            configuration_revision,
        ) = await self._build_request(request)
        requires_finalizer = (
            request.step_type == "execute_code" and "step09a_capsule" in provider_request.task_draft
        )
        if requires_finalizer and (self._finalizer is None or self._worktrees is None):
            async with self._factory.begin() as session:
                await release_reservation(
                    session, reservation_id=reservation_id, token=request.lease
                )
            return StepOutcome(
                kind=OutcomeKind.PERMANENT_FAILURE,
                result={},
                category="worker_finalizer_unavailable",
                summary="deterministic worker finalizer is unavailable",
            )
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

        finalized: FinalizedWorkerResult | None = None
        finalization_failure: WorkerFinalizationError | None = None
        invalid_edit_report = False
        if (
            request.step_type == "execute_code"
            and "step09a_capsule" in provider_request.task_draft
            and self._finalizer is not None
            and self._worktrees is not None
        ):
            try:
                finalized = await self._finalize_worker_result(
                    request=request,
                    provider_request=provider_request,
                    profile_id=profile.id,
                    result=result,
                    cancellation=cancellation,
                )
            except WorkerFinalizationError as error:
                finalization_failure = error
            except ValidationError:
                invalid_edit_report = True
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
            finalization_result = (
                finalized
                if finalized is not None
                else finalization_failure.result
                if finalization_failure is not None
                else None
            )
            if finalization_result is not None and self._finalizer is not None:
                await self._finalizer.persist(
                    session,
                    task_id=request.task_id,
                    run_id=request.run_id,
                    step_id=request.step_id,
                    result=finalization_result,
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
        if finalization_failure is not None:
            return StepOutcome(
                kind=OutcomeKind.PERMANENT_FAILURE,
                result={},
                category=finalization_failure.category,
                summary=finalization_failure.safe_summary,
                unknown_effects=False,
            )
        if invalid_edit_report:
            return StepOutcome(
                kind=OutcomeKind.PERMANENT_FAILURE,
                result={},
                category="invalid_worker_edit_report",
                summary="provider returned an invalid worker edit report",
                unknown_effects=False,
            )
        structured_output = (
            finalized.manifest.model_dump(mode="json")
            if finalized is not None
            else result.structured_output
        )
        return StepOutcome.succeeded(
            {
                "profile_id": profile.id,
                "model": profile.model,
                "provider_request_id": result.provider_request_id,
                "text": result.text,
                "structured_output": structured_output,
                "finish_reason": result.finish_reason,
            }
        )

    async def _finalize_worker_result(
        self,
        *,
        request: StepExecutionRequest,
        provider_request: ProviderRequest,
        profile_id: str,
        result: ProviderResult,
        cancellation: CancellationContext,
    ) -> FinalizedWorkerResult:
        from vuzol.experiments.domain import WorkerEditReport, WorkerTaskCapsule

        if self._finalizer is None or self._worktrees is None:
            raise RuntimeError("deterministic worker finalizer is unavailable")
        capsule = WorkerTaskCapsule.model_validate(provider_request.task_draft["step09a_capsule"])
        edit_report = WorkerEditReport.model_validate(result.structured_output)
        async with self._factory() as session:
            worktree = await self._worktrees.reference_for_run(session, run_id=request.run_id)
        return await self._finalizer.finalize(
            worktree=worktree.path,
            capsule=capsule,
            edit_report=edit_report,
            worker_profile=profile_id,
            provider_usage=result.usage,
            provider_attempt=provider_request.provider_attempt,
            gate_context=GateExecutionContext(
                task_id=request.task_id,
                run_id=request.run_id,
                step_id=request.step_id,
                worktree_id=worktree.id,
                profile_id=profile_id,
                provider_attempt=provider_request.provider_attempt,
                lease_generation=request.lease.generation,
            ),
            cancellation=cancellation,
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
            output_schema_name, output_schema_version, output_json_schema = _step09a_result_schema(
                step.step_type, task.task_draft
            )
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
                    output_schema_name=output_schema_name,
                    output_schema_version=output_schema_version,
                    output_json_schema=output_json_schema,
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


def _step09a_result_schema(
    step_type: str, task_draft: dict[str, object]
) -> tuple[str | None, str | None, dict[str, object] | None]:
    if step_type != "execute_code" or "step09a_capsule" not in task_draft:
        return None, None, None
    from vuzol.experiments.domain import WorkerEditReport

    return (
        "WorkerEditReport",
        "step09a-worker-edit-report.v1",
        WorkerEditReport.model_json_schema(),
    )
