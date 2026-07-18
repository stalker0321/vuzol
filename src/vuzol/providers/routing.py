"""Transactional provider routing, budget reservation, and fenced step claim."""

import json
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import (
    BudgetMode,
    Capability,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings
from vuzol.providers.budgets import BudgetExceeded, estimate_reservation, reserve_budget
from vuzol.providers.domain import EffectiveProfileState
from vuzol.providers.health import effective_health
from vuzol.providers.policy import (
    ExclusionReason,
    PolicyDecision,
    RoutingRequest,
    select_profile,
)
from vuzol.storage.leasing import STEP_CLAIM_LOCK_KEY
from vuzol.storage.models import RoutingDecision, Run, Step, Task
from vuzol.storage.records import LeaseToken
from vuzol.storage.repositories.core import step_record
from vuzol.storage.types import QueueClass, RunStatus, StepStatus, TaskStatus
from vuzol.telegram.projections import (
    enqueue_project_status_dashboard,
    enqueue_terminal_task_projections,
)
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

PROVIDER_STEP_ROLES: dict[str, ProviderRole] = {
    "execute_model": ProviderRole.EXECUTOR,
    "research_execute": ProviderRole.EXECUTOR,
    "synthesize": ProviderRole.SUMMARIZER,
    "plan": ProviderRole.PLANNER,
    "review": ProviderRole.REVIEWER,
    "execute_code": ProviderRole.EXECUTOR,
    "execute_agent": ProviderRole.EXECUTOR,
}

# Content-quality plan failures are not profile outages; the same planner may retry.
PLANNER_CONTENT_FAILURE_CATEGORIES = frozenset(
    {
        "planner_empty_output",
        "planner_truncated",
        "planner_invalid_output",
    }
)


async def claim_routed_step(
    session: AsyncSession,
    *,
    settings: Settings,
    registries: ConfigurationBundle,
    owner: str,
    lease_seconds: int,
    candidate_limit: int,
    class_limits: dict[QueueClass, int] | None = None,
    step_types: frozenset[str] = frozenset(PROVIDER_STEP_ROLES),
) -> LeaseToken | None:
    if candidate_limit < 1 or not step_types:
        return None
    await session.execute(select(func.pg_advisory_xact_lock(STEP_CLAIM_LOCK_KEY)))
    rows = await session.execute(
        select(Step, Run, Task)
        .join(Run, Run.id == Step.run_id)
        .join(Task, Task.id == Run.task_id)
        .where(
            Step.status == StepStatus.QUEUED,
            Step.available_at <= func.now(),
            Step.attempt_count < Step.max_attempts,
            Step.step_type.in_(sorted(step_types)),
            Run.status == RunStatus.RUNNING,
        )
        .order_by(Step.priority, Step.available_at, Step.created_at)
        .with_for_update(skip_locked=True)
        .limit(candidate_limit)
    )
    profiles = registries.profiles.items()
    for step, run, task in tuple(rows.all()):
        if class_limits is not None and (limit := class_limits.get(step.queue_class)) is not None:
            active_class = int(
                await session.scalar(
                    select(func.count())
                    .select_from(Step)
                    .where(
                        Step.queue_class == step.queue_class,
                        Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                        Step.lease_expires_at >= func.now(),
                    )
                )
                or 0
            )
            if active_class >= limit:
                continue
        decision_count = int(
            await session.scalar(
                select(func.count())
                .select_from(RoutingDecision)
                .where(RoutingDecision.step_id == step.id)
            )
            or 0
        )
        provider_call_count = int(
            await session.scalar(
                select(func.count())
                .select_from(RoutingDecision)
                .where(
                    RoutingDecision.step_id == step.id,
                    RoutingDecision.selected_profile_id.is_not(None),
                )
            )
            or 0
        )
        attempt = decision_count + 1
        if provider_call_count >= settings.limits.provider_attempts:
            await _block_route(
                session,
                step=step,
                run=run,
                task=task,
                category="provider_attempts_exhausted",
                quota=False,
            )
            continue
        required = frozenset(Capability(value) for value in step.required_capabilities)
        project_caps = None
        if task.project_id is not None:
            project_caps = registries.projects.get(task.project_id).allowed_capabilities
        states = await _states(
            session,
            profiles,
            configuration_revision=run.configuration_revision,
        )
        role = PROVIDER_STEP_ROLES[step.step_type]
        estimated_input = _estimated_input_tokens(task)
        requested_output = (
            settings.limits.planner_output_tokens
            if role is ProviderRole.PLANNER
            else settings.limits.provider_call_output_tokens
        )
        failed_profile_id = (
            step.executor_profile_id
            if (
                attempt > 1
                and step.failure_category is not None
                and step.failure_category not in PLANNER_CONTENT_FAILURE_CATEGORIES
            )
            else None
        )
        allowed_fallbacks: tuple[str, ...] = ()
        if failed_profile_id is not None:
            allowed_fallbacks = registries.profiles.get(failed_profile_id).fallback_profile_ids
        policy_request = RoutingRequest(
            role=role,
            task_type=task.task_type,
            required_capabilities=required,
            project_allowed_capabilities=project_caps,
            budget_mode=BudgetMode(run.budget_mode),
            estimated_input_tokens=estimated_input,
            max_output_tokens=requested_output,
            remaining_cost_units=settings.limits.task_cost_units,
            trusted_profile_id=_trusted_profile_id(run),
            failed_profile_id=failed_profile_id,
            allowed_fallback_ids=allowed_fallbacks,
            requires_sandbox=step.step_type in {"execute_code", "execute_agent"},
            required_launch_mode=(
                LaunchMode.CLI
                if step.step_type in {"execute_code", "execute_agent"}
                else LaunchMode.API
            ),
        )
        decision = select_profile(policy_request, profiles, states)
        ordered = tuple(
            profile_id
            for profile_id in (decision.selected_profile_id, *decision.alternatives)
            if profile_id is not None
        )
        if not ordered and _temporarily_unavailable(decision):
            continue
        selected: ProviderProfileConfig | None = None
        reservation = None
        for profile_id in ordered:
            profile = registries.profiles.get(profile_id)
            estimate = estimate_reservation(
                profile,
                input_tokens=min(estimated_input, settings.limits.provider_call_input_tokens),
                output_tokens=min(requested_output, profile.output_limit or requested_output),
            )
            try:
                reservation = await reserve_budget(
                    session,
                    task_id=task.id,
                    run_id=run.id,
                    step_id=step.id,
                    profile_id=profile.id,
                    provider_attempt=attempt,
                    estimate=estimate,
                    limits=settings.limits,
                )
            except BudgetExceeded:
                continue
            selected = profile
            break
        if selected is None or reservation is None:
            exhausted = bool(ordered) or _quota_exhausted(decision)
            await _persist_decision(
                session,
                run=run,
                step=step,
                role=role,
                attempt=attempt,
                selected_profile_id=None,
                decision=decision,
                request=policy_request,
            )
            await _block_route(
                session,
                step=step,
                run=run,
                task=task,
                category="budget_exhausted" if exhausted else "no_compatible_profile",
                quota=exhausted,
            )
            continue
        await _persist_decision(
            session,
            run=run,
            step=step,
            role=role,
            attempt=attempt,
            selected_profile_id=selected.id,
            decision=decision,
            request=policy_request,
        )
        step.executor_profile_id = selected.id
        step.status = StepStatus.LEASED
        step.lease_owner = owner
        step.lease_generation += 1
        step.heartbeat_at = func.now()
        step.lease_expires_at = func.now() + timedelta(seconds=lease_seconds)
        step.attempt_count += 1
        step.payload = {
            **step.payload,
            "provider_attempt": attempt,
            "budget_reservation_id": str(reservation.id),
        }
        await session.flush()
        await session.refresh(step, attribute_names=["heartbeat_at", "lease_expires_at"])
        if task.source_chat_id:
            await enqueue_project_status_dashboard(session, task.source_chat_id)
        return LeaseToken(step=step_record(step), owner=owner, generation=step.lease_generation)
    return None


def _trusted_profile_id(run: Run) -> str | None:
    """Read only the bounded internal Step 09A route pin, never model task output."""
    if run.selected_route.get("schema_version") != "step09a-route.v1":
        return None
    value = run.selected_route.get("trusted_profile_id")
    return value if isinstance(value, str) else None


async def _states(
    session: AsyncSession,
    profiles: tuple[ProviderProfileConfig, ...],
    *,
    configuration_revision: str,
) -> dict[str, EffectiveProfileState]:
    result: dict[str, EffectiveProfileState] = {}
    for profile in profiles:
        state = await effective_health(
            session, profile, configuration_revision=configuration_revision
        )
        active = int(
            await session.scalar(
                select(func.count())
                .select_from(Step)
                .where(
                    Step.executor_profile_id == profile.id,
                    Step.status.in_((StepStatus.LEASED, StepStatus.RUNNING)),
                    Step.lease_expires_at >= func.now(),
                )
            )
            or 0
        )
        queued = int(
            await session.scalar(
                select(func.count())
                .select_from(Step)
                .where(
                    Step.executor_profile_id == profile.id,
                    Step.status == StepStatus.QUEUED,
                )
            )
            or 0
        )
        result[profile.id] = state.model_copy(
            update={"active_leases": active, "queue_depth": queued}
        )
    return result


async def _persist_decision(
    session: AsyncSession,
    *,
    run: Run,
    step: Step,
    role: ProviderRole,
    attempt: int,
    selected_profile_id: str | None,
    decision: PolicyDecision,
    request: RoutingRequest,
) -> None:
    evaluations = [
        {
            "profile_id": evaluation.profile_id,
            "eligible": evaluation.eligible,
            "reasons": [reason.value for reason in evaluation.reasons],
        }
        for evaluation in decision.evaluations
    ]
    session.add(
        RoutingDecision(
            run_id=run.id,
            step_id=step.id,
            provider_attempt=attempt,
            decision_kind="fallback" if request.failed_profile_id is not None else "initial",
            role=role.value,
            selected_profile_id=selected_profile_id,
            alternatives=[
                {"profile_id": profile_id, "rank": rank}
                for rank, profile_id in enumerate(decision.alternatives, start=1)
            ],
            inputs={
                "task_type": request.task_type,
                "required_capabilities": sorted(
                    capability.value for capability in request.required_capabilities
                ),
                "budget_mode": request.budget_mode.value,
                "evaluations": evaluations,
            },
            policy_revision=run.policy_revision,
        )
    )
    await session.flush()


async def _block_route(
    session: AsyncSession,
    *,
    step: Step,
    run: Run,
    task: Task,
    category: str,
    quota: bool,
) -> None:
    summary = (
        "Доступный лимит провайдера исчерпан; задача ожидает возобновления квоты."
        if quota
        else "Маршрутизатор не нашёл безопасного доступного исполнителя для этого этапа."
    )
    await transition_step(
        session,
        step,
        StepStatus.BLOCKED,
        actor_type="routing_policy",
        payload={"category": category},
    )
    step.failure_category = category
    step.failure_summary = summary
    await transition_run(
        session,
        run,
        RunStatus.BLOCKED,
        actor_type="routing_policy",
        payload={"category": category},
    )
    run.failure_category = category
    run.failure_summary = summary
    await transition_task(
        session,
        task,
        TaskStatus.QUOTA_EXHAUSTED if quota else TaskStatus.BLOCKED,
        actor_type="routing_policy",
        payload={"category": category},
    )
    if not quota:
        await enqueue_terminal_task_projections(session, task, run)


def _estimated_input_tokens(task: Task) -> int:
    characters = len(task.original_text) + len(
        json.dumps(task.task_draft, ensure_ascii=False, sort_keys=True)
    )
    return max(1, (characters + 3) // 4)


def _temporarily_unavailable(decision: PolicyDecision) -> bool:
    transient = {
        ExclusionReason.CONCURRENCY,
        ExclusionReason.RATE_LIMITED,
    }
    return any(
        evaluation.reasons and set(evaluation.reasons).issubset(transient)
        for evaluation in decision.evaluations
    )


def _quota_exhausted(decision: PolicyDecision) -> bool:
    exhausted = {ExclusionReason.QUOTA, ExclusionReason.BUDGET}
    return any(
        evaluation.reasons and set(evaluation.reasons).issubset(exhausted)
        for evaluation in decision.evaluations
    )
