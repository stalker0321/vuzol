"""Step 06 handler adapter for safe model-only provider calls."""

import uuid
from decimal import Decimal
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import Capability, ProviderProfileConfig
from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.access import (
    WorktreeAccessError,
    WorktreeAccessLease,
    WorktreeAccessManager,
)
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.finalization import (
    FinalizedWorkerResult,
    GateExecutionContext,
    WorkerFinalizationError,
    WorkerFinalizer,
)
from vuzol.execution.worktrees import WorktreeService
from vuzol.observability import get_logger
from vuzol.providers.budgets import account_usage, reconcile_usage, release_reservation
from vuzol.providers.domain import ProviderErrorCategory, ProviderRequest, ProviderResult
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.health import record_failure_observation, record_success_observation
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.registry import AdapterRegistry
from vuzol.providers.routing import PROVIDER_STEP_ROLES
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import (
    ProviderBudgetReservation,
    Run,
    Step,
    SupervisedProcess,
    Task,
    Worktree,
)
from vuzol.storage.types import BudgetReservationStatus, StepStatus, WorktreeDeliveryState
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
        worktree_access: WorktreeAccessManager | None = None,
    ) -> None:
        self._factory = factory
        self._registries = registries
        self._adapters = adapters
        self._worktrees = worktrees
        self._artifacts = artifacts
        self._finalizer = finalizer
        self._worktree_access = worktree_access

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        reservation_id = _reservation_id(request.payload)
        try:
            built = (
                provider_request,
                profile_id,
                reservation_id,
                _configuration_revision,
            ) = await self._build_request(request)
            profile = self._registries.profiles.get(profile_id)
            adapter = self._adapters.get(profile_id)
        except (LookupError, ValueError) as error:
            return await self._pre_provider_failure(
                request,
                reservation_id=reservation_id,
                category="provider_request_invalid",
                error=error,
            )
        except Exception as error:
            return await self._unexpected_pre_provider_failure(
                request,
                reservation_id=reservation_id,
                error=error,
            )
        requires_finalizer = (
            request.step_type == "execute_code" and "step09a_capsule" in provider_request.task_draft
        )
        if requires_finalizer and (
            self._finalizer is None or self._worktrees is None or self._worktree_access is None
        ):
            return await self._pre_provider_failure(
                request,
                reservation_id=reservation_id,
                category="worker_finalizer_unavailable",
                summary="deterministic worker finalizer is unavailable",
            )
        access: WorktreeAccessLease | None = None
        if requires_finalizer:
            assert self._worktree_access is not None
            try:
                access = await self._grant_worktree_access(request)
            except (LookupError, ValueError, WorktreeAccessError) as error:
                return await self._pre_provider_failure(
                    request,
                    reservation_id=reservation_id,
                    category="worker_access_unavailable",
                    error=error,
                )
            except Exception as error:
                return await self._unexpected_pre_provider_failure(
                    request,
                    reservation_id=reservation_id,
                    error=error,
                )
        try:
            return await self._execute_built(
                request,
                cancellation,
                built=built,
                profile=profile,
                adapter=adapter,
                access=access,
            )
        except Exception as error:
            if await self._provider_launch_exists(request):
                return await self._unexpected_launched_provider_failure(
                    request,
                    reservation_id=reservation_id,
                    profile=profile,
                    configuration_revision=_configuration_revision,
                    error=error,
                )
            return await self._unexpected_pre_provider_failure(
                request,
                reservation_id=reservation_id,
                error=error,
            )
        finally:
            if access is not None:
                await access.revoke()

    async def _execute_built(
        self,
        request: StepExecutionRequest,
        cancellation: CancellationContext,
        *,
        built: tuple[ProviderRequest, str, uuid.UUID, str],
        profile: ProviderProfileConfig,
        adapter: ProviderAdapter,
        access: WorktreeAccessLease | None,
    ) -> StepOutcome:
        provider_request, _profile_id, reservation_id, configuration_revision = built
        try:
            result = await adapter.execute(provider_request, profile, cancellation)
        except ProviderFailure as failure:
            if not failure.request_sent:
                return await self._pre_provider_failure(
                    request,
                    reservation_id=reservation_id,
                    category=failure.category.value,
                    summary=failure.safe_summary,
                )
            async with self._factory.begin() as session:
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
                await record_failure_observation(
                    session,
                    profile,
                    configuration_revision=configuration_revision,
                    failure=failure,
                )
            await self._retain_active_worktree(request)
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
                    access=access,
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
        access: WorktreeAccessLease | None,
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
            access=access,
        )

    async def _pre_provider_failure(
        self,
        request: StepExecutionRequest,
        *,
        reservation_id: uuid.UUID | None,
        category: str,
        error: Exception | None = None,
        summary: str | None = None,
    ) -> StepOutcome:
        safe_summary = summary or (
            type(error).__name__ if error is not None else "provider preparation failed"
        )
        try:
            await self._unwind_pre_provider(request, reservation_id=reservation_id)
        except Exception as unwind_error:
            get_logger(__name__).error(
                "provider.pre_provider_unwind_failed",
                extra={
                    "task_id": str(request.task_id),
                    "run_id": str(request.run_id),
                    "step_id": str(request.step_id),
                    "lease_generation": request.lease.generation,
                    "preparation_category": category,
                    "preparation_error_type": (type(error).__name__ if error is not None else None),
                    "unwind_error_type": type(unwind_error).__name__,
                    "unwind_error_location": _safe_exception_location(unwind_error),
                },
            )
            return StepOutcome(
                kind=OutcomeKind.PERMANENT_FAILURE,
                result={},
                category="pre_provider_unwind_failed",
                summary=(f"{category} followed by unwind failure ({type(unwind_error).__name__})"),
                unknown_effects=False,
            )
        return StepOutcome(
            kind=OutcomeKind.PERMANENT_FAILURE,
            result={},
            category=category,
            summary=safe_summary,
            unknown_effects=False,
        )

    async def _unexpected_pre_provider_failure(
        self,
        request: StepExecutionRequest,
        *,
        reservation_id: uuid.UUID | None,
        error: Exception,
    ) -> StepOutcome:
        get_logger(__name__).error(
            "provider.pre_provider_unexpected",
            extra={
                "task_id": str(request.task_id),
                "run_id": str(request.run_id),
                "step_id": str(request.step_id),
                "lease_generation": request.lease.generation,
                "error_type": type(error).__name__,
                "error_location": _safe_exception_location(error),
            },
        )
        return await self._pre_provider_failure(
            request,
            reservation_id=reservation_id,
            category="pre_provider_unexpected",
            error=error,
        )

    async def _unwind_pre_provider(
        self,
        request: StepExecutionRequest,
        *,
        reservation_id: uuid.UUID | None,
    ) -> None:
        async with self._factory.begin() as session:
            run = await session.scalar(
                select(Run)
                .where(Run.id == request.run_id, Run.task_id == request.task_id)
                .with_for_update()
            )
            step = await session.scalar(
                select(Step)
                .where(
                    Step.id == request.step_id,
                    Step.run_id == request.run_id,
                    Step.lease_owner == request.lease.owner,
                    Step.lease_generation == request.lease.generation,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                )
                .with_for_update()
            )
            if run is None or step is None:
                raise LeaseLost(
                    f"step lease lost before provider preparation unwind: {request.step_id}"
                )

            reservation = await self._reservation_for_unwind(
                session,
                request=request,
                reservation_id=reservation_id,
            )
            if reservation is not None:
                await release_reservation(
                    session,
                    reservation_id=reservation.id,
                    token=request.lease,
                )

            worktree = await session.scalar(
                select(Worktree)
                .where(
                    Worktree.run_id == request.run_id,
                    Worktree.task_id == request.task_id,
                )
                .with_for_update()
            )
            if worktree is not None and worktree.delivery_state is WorktreeDeliveryState.ACTIVE:
                if self._worktrees is None:
                    raise RuntimeError("worktree service is unavailable during provider unwind")
                await self._worktrees.retain(
                    session,
                    worktree_id=worktree.id,
                    artifacts=self._artifacts,
                    step_id=request.step_id,
                )

    async def _reservation_for_unwind(
        self,
        session: AsyncSession,
        *,
        request: StepExecutionRequest,
        reservation_id: uuid.UUID | None,
    ) -> ProviderBudgetReservation | None:
        constraints = (
            ProviderBudgetReservation.task_id == request.task_id,
            ProviderBudgetReservation.run_id == request.run_id,
            ProviderBudgetReservation.step_id == request.step_id,
        )
        if reservation_id is not None:
            reservation: ProviderBudgetReservation | None = await session.scalar(
                select(ProviderBudgetReservation)
                .where(ProviderBudgetReservation.id == reservation_id, *constraints)
                .with_for_update()
            )
            if reservation is not None:
                return reservation
        attempt = request.payload.get("provider_attempt")
        statement = select(ProviderBudgetReservation).where(*constraints)
        if isinstance(attempt, int) and not isinstance(attempt, bool):
            statement = statement.where(ProviderBudgetReservation.provider_attempt == attempt)
        else:
            statement = statement.where(
                ProviderBudgetReservation.status == BudgetReservationStatus.RESERVED
            )
        reservation = await session.scalar(statement.with_for_update())
        return reservation

    async def _provider_launch_exists(self, request: StepExecutionRequest) -> bool:
        async with self._factory() as session:
            return (
                await session.scalar(
                    select(SupervisedProcess.id).where(
                        SupervisedProcess.task_id == request.task_id,
                        SupervisedProcess.run_id == request.run_id,
                        SupervisedProcess.step_id == request.step_id,
                        SupervisedProcess.lease_generation == request.lease.generation,
                    )
                )
                is not None
            )

    async def _unexpected_launched_provider_failure(
        self,
        request: StepExecutionRequest,
        *,
        reservation_id: uuid.UUID,
        profile: ProviderProfileConfig,
        configuration_revision: str,
        error: Exception,
    ) -> StepOutcome:
        get_logger(__name__).error(
            "provider.execution_unexpected",
            extra={
                "task_id": str(request.task_id),
                "run_id": str(request.run_id),
                "step_id": str(request.step_id),
                "lease_generation": request.lease.generation,
                "error_type": type(error).__name__,
                "error_location": _safe_exception_location(error),
            },
        )
        failure = ProviderFailure(
            ProviderErrorCategory.UNKNOWN,
            retryable=False,
            request_sent=True,
            safe_summary="provider execution failed unexpectedly",
        )
        async with self._factory.begin() as session:
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
            await record_failure_observation(
                session,
                profile,
                configuration_revision=configuration_revision,
                failure=failure,
            )
        await self._retain_active_worktree(request)
        return StepOutcome(
            kind=OutcomeKind.PERMANENT_FAILURE,
            result={},
            category="provider_execution_unexpected",
            summary=type(error).__name__,
            unknown_effects=False,
        )

    async def _retain_active_worktree(self, request: StepExecutionRequest) -> None:
        if request.step_type != "execute_code" or self._worktrees is None:
            return
        async with self._factory.begin() as session:
            worktree = await session.scalar(
                select(Worktree)
                .where(
                    Worktree.run_id == request.run_id,
                    Worktree.task_id == request.task_id,
                )
                .with_for_update()
            )
            if worktree is not None and worktree.delivery_state is WorktreeDeliveryState.ACTIVE:
                await self._worktrees.retain(
                    session,
                    worktree_id=worktree.id,
                    artifacts=self._artifacts,
                    step_id=request.step_id,
                )

    async def _grant_worktree_access(self, request: StepExecutionRequest) -> WorktreeAccessLease:
        if self._worktree_access is None:
            raise RuntimeError("worktree access manager is unavailable")
        async with self._factory() as session:
            worktree = await session.scalar(
                select(Worktree).where(Worktree.run_id == request.run_id)
            )
            if worktree is None:
                raise LookupError("execute_code requires a prepared worktree")
            project = self._registries.projects.get(worktree.project_id)
            if project.validation_sandbox_profile is None:
                raise WorktreeAccessError(f"project {project.id} has no validation sandbox profile")
            validation = self._registries.sandboxes.get(project.validation_sandbox_profile)
            if not validation.enabled:
                raise WorktreeAccessError(f"project {project.id} validation sandbox is disabled")
            sandbox = self._registries.sandboxes.get(project.sandbox_profile)
        return await self._worktree_access.grant(
            Path(worktree.path), sandbox_uid=sandbox.uid, sandbox_gid=sandbox.gid
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
            if step.run_id != run.id or run.task_id != task.id:
                raise LookupError("routed provider step identity is inconsistent")
            if (
                step.id != request.step_id
                or run.id != request.run_id
                or task.id != request.task_id
                or step.lease_owner != request.lease.owner
                or step.lease_generation != request.lease.generation
                or step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
            ):
                raise LookupError("routed provider step lease is invalid")
            reservation_value = step.payload.get("budget_reservation_id")
            attempt_value = step.payload.get("provider_attempt")
            if (
                not isinstance(reservation_value, str)
                or not isinstance(attempt_value, int)
                or isinstance(attempt_value, bool)
            ):
                raise LookupError("provider reservation reference is missing")
            reservation_id = uuid.UUID(reservation_value)
            reservation = await session.get(ProviderBudgetReservation, reservation_id)
            if (
                reservation is None
                or reservation.task_id != task.id
                or reservation.run_id != run.id
                or reservation.step_id != step.id
                or reservation.profile_id != step.executor_profile_id
                or reservation.provider_attempt != attempt_value
                or reservation.status is not BudgetReservationStatus.RESERVED
            ):
                raise LookupError("provider reservation is missing")
            profile = self._registries.profiles.get(step.executor_profile_id)
            worktree = None
            if step.step_type == "execute_code":
                worktree = await session.scalar(
                    select(Worktree).where(
                        Worktree.run_id == run.id,
                        Worktree.task_id == task.id,
                    )
                )
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


def _reservation_id(payload: dict[str, object]) -> uuid.UUID | None:
    value = payload.get("budget_reservation_id")
    if not isinstance(value, str):
        return None
    try:
        return uuid.UUID(value)
    except (AttributeError, TypeError, ValueError):
        return None


def _safe_exception_location(error: Exception) -> str | None:
    traceback = error.__traceback__
    if traceback is None:
        return None
    while traceback.tb_next is not None:
        traceback = traceback.tb_next
    code = traceback.tb_frame.f_code
    return f"{Path(code.co_filename).name}:{code.co_name}:{traceback.tb_lineno}"


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
