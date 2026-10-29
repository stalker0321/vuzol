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
from vuzol.config.registries import ConfigurationBundle, RegistryError
from vuzol.config.revision import assignment_matches
from vuzol.config.settings import Settings
from vuzol.context.execution import execution_constraints_context
from vuzol.ops.disk_pressure import (
    FreeSpaceProbe,
    assess_heavy_claim_gate,
    report_claim_disk_pressure_deferred,
)
from vuzol.projects.executor_preference import (
    ExecutorPreferenceError,
    ExecutorRoutePin,
    load_preference,
    preference_payload,
    resolve_route_pin,
    same_family_fallback_ids,
)
from vuzol.projects.model_matrix import (
    MATRIX_KEY,
    assignment_profile,
    legacy_executor_preference,
    pinned_assignment,
    slot_for_role,
)
from vuzol.providers.budgets import (
    ADMISSION_ONLY_CONSENT_SUMMARY,
    EXPLICIT_MONEY_CAP_EXCEEDED_SUMMARY,
    EXPLICIT_MONEY_CAP_UNKNOWN_SUMMARY,
    BudgetExceeded,
    GoalBudgetExceeded,
    accounting_for_profile,
    attempt_kind_for_step,
    estimate_reservation,
    note_admission_only_ceiling,
    purpose_for_step_type,
    remaining_output_allowance,
    reserve_budget,
    resolve_horizon_scope,
)
from vuzol.providers.catalog import CONVERSATION_TASK_TYPE
from vuzol.providers.control import ceiling_is_admission_only
from vuzol.providers.domain import EffectiveProfileState
from vuzol.providers.fallback_policy import should_fallback_provider
from vuzol.providers.health import effective_health
from vuzol.providers.policy import (
    ExclusionReason,
    PolicyDecision,
    RoutingRequest,
    select_profile,
)
from vuzol.providers.result_schema import result_schema_for_step, schema_char_count
from vuzol.storage.leasing import STEP_CLAIM_LOCK_KEY
from vuzol.storage.models import ProviderBudgetReservation, RoutingDecision, Run, Step, Task
from vuzol.storage.records import LeaseToken
from vuzol.storage.repositories.core import step_record
from vuzol.storage.types import QueueClass, RunStatus, StepStatus, TaskStatus
from vuzol.telegram.projections import (
    enqueue_project_status_dashboard,
    enqueue_terminal_task_projections,
)
from vuzol.workflows.transitions import transition_run, transition_step, transition_task

PROVIDER_STEP_ROLES: dict[str, ProviderRole] = {
    "research_execute": ProviderRole.EXECUTOR,
    "synthesize": ProviderRole.SUMMARIZER,
    "review": ProviderRole.REVIEWER,
    "execute_code": ProviderRole.EXECUTOR,
    "execute_agent": ProviderRole.EXECUTOR,
}


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
    free_space_probe: FreeSpaceProbe | None = None,
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
    heavy_disk_blocked: bool | None = None
    for step, run, task in tuple(rows.all()):
        if step.queue_class is QueueClass.HEAVY:
            if heavy_disk_blocked is None:
                # settings is required on this API; assess_heavy_claim_gate still
                # fail-closes if a caller ever passes None via type hole.
                assessment = assess_heavy_claim_gate(settings, probe=free_space_probe)
                heavy_disk_blocked = assessment.blocked
                if heavy_disk_blocked:
                    report_claim_disk_pressure_deferred(assessment)
            if heavy_disk_blocked:
                # Defer without routing decision, budget reservation, or attempt burn.
                continue
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
                .outerjoin(
                    ProviderBudgetReservation,
                    (ProviderBudgetReservation.step_id == RoutingDecision.step_id)
                    & (
                        ProviderBudgetReservation.provider_attempt
                        == RoutingDecision.provider_attempt
                    ),
                )
                .where(
                    RoutingDecision.step_id == step.id,
                    RoutingDecision.selected_profile_id.is_not(None),
                    # Explicit owner continuation admits a fresh bounded epoch.
                    # Lifetime attempt IDs/history remain; unreserved legacy
                    # rows belong to epoch zero and consume its attempt cap.
                    func.coalesce(ProviderBudgetReservation.budget_epoch, 0) == task.budget_epoch,
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
        # Preserve old, unbound discussion runs. A validated conversation snapshot
        # below uses its own internal purpose instead of this legacy task alias.
        routing_task_type = (
            "architecture" if task.task_type == "discussion_agent_internal" else task.task_type
        )
        from vuzol.discussion.work_context import load_work_context

        try:
            work_context = await load_work_context(
                session,
                task,
                expected_revision=step.payload.get("chat_work_spec_revision")
                or run.selected_route.get("chat_work_spec_revision"),
            )
        except ValueError:
            await _block_route(
                session,
                step=step,
                run=run,
                task=task,
                category="chat_work_context_invalid",
                quota=False,
            )
            continue
        estimated_input = _estimated_input_tokens(task, step, run)
        estimated_input += (sum(len(item.content) for item in work_context) + 3) // 4
        if step.step_type == "execute_code":
            facts = execution_constraints_context(
                registries.projects.get(task.project_id) if task.project_id else None,
                dependency_provisioning_enabled=settings.dependency_provisioning.enabled,
            )
            if facts is not None:
                estimated_input += (len(facts.content) + 3) // 4
        if work_context and estimated_input > settings.limits.provider_call_input_tokens:
            await _block_route(
                session,
                step=step,
                run=run,
                task=task,
                category="chat_work_context_budget_exceeded",
                quota=False,
                summary="Full discussion context exceeds the provider input budget.",
            )
            continue
        requested_output = settings.limits.provider_call_output_tokens
        requested_output = await remaining_output_allowance(
            session,
            task_id=task.id,
            step_id=step.id,
            budget_epoch=task.budget_epoch,
            requested=requested_output,
            limits=settings.limits,
        )
        if requested_output == 0:
            await _block_route(
                session, step=step, run=run, task=task, category="budget_exhausted", quota=True
            )
            continue
        retry_failed_profile_id = step.payload.get("retry_failed_profile_id")
        failed_profile_id = (
            (
                retry_failed_profile_id
                if isinstance(retry_failed_profile_id, str)
                else step.executor_profile_id
            )
            if (
                attempt > 1
                and (
                    isinstance(retry_failed_profile_id, str)
                    or should_fallback_provider(step.failure_category)
                )
            )
            else None
        )
        allowed_fallbacks: tuple[str, ...] = ()
        if failed_profile_id is not None:
            try:
                allowed_fallbacks = registries.profiles.get(failed_profile_id).fallback_profile_ids
            except RegistryError:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="project_pin_unresolved",
                    quota=False,
                    summary="Previously selected provider profile is unavailable.",
                )
                continue
            # A pin without configured fallbacks must still be able to consume its
            # bounded retry attempts. Excluding its only profile turns retries into
            # immediate no_compatible_profile failures without calling the provider.
            if not allowed_fallbacks:
                failed_profile_id = None
        project_pin = None
        pin_family_ids: frozenset[str] | None = None
        pin_preference_revision: int | None = None
        pin_worker_key: str | None = None
        chat_profile_id = None
        if run.selected_route.get("schema_version") == "discussion-chat-route.v1":
            value = run.selected_route.get("trusted_profile_id")
            if (
                task.task_type != "discussion_agent_internal"
                or run.workflow_type != "discussion_agent"
                or step.step_type != "execute_agent"
                or required != frozenset({Capability.REPOSITORY_READ})
                or not isinstance(value, str)
                or not value
            ):
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="chat_route_invalid",
                    quota=False,
                )
                continue
            chat_profile_id = value
            from vuzol.discussion.conversation import conversation_provider_role

            try:
                role = conversation_provider_role(run.selected_route)
            except ValueError:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="chat_route_invalid",
                    quota=False,
                )
                continue
            responder = next((p for p in profiles if p.id == value), None)
            if responder is None or not assignment_matches(
                responder, run.selected_route.get("profile_revision")
            ):
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="chat_profile_changed",
                    quota=False,
                )
                continue
            pin_family_ids = frozenset({value})
            # Conversation has its own already-validated internal purpose.
            # Do not pretend that an arbitrary lead/think model is an architecture worker.
            routing_task_type = CONVERSATION_TASK_TYPE
            # An exact responder never uses project /model or any profile fallback.
            failed_profile_id = None
            allowed_fallbacks = ()
        # Pins apply only to CLI coding/agent sandboxes — never API/research executors.
        pin_eligible_step = (
            step.step_type in {"execute_code", "execute_agent"} and chat_profile_id is None
        )
        matrix_slot = slot_for_role(role, strong=run.budget_mode == "strong")
        if chat_profile_id is not None or (role is ProviderRole.EXECUTOR and not pin_eligible_step):
            matrix_slot = None
        matrix_profile: ProviderProfileConfig | None = None
        try:
            matrix_choice = pinned_assignment(run.selected_route, matrix_slot)
            if matrix_choice is not None and matrix_slot is not None:
                matrix_profile = assignment_profile(matrix_choice, registries, matrix_slot)
            preference = (
                legacy_executor_preference(run.selected_route)
                if MATRIX_KEY in run.selected_route
                else await load_preference(session, task.project_id)
                if pin_eligible_step and task.project_id is not None
                else None
            )
        except (ValueError, KeyError, ExecutorPreferenceError):
            await _block_route(
                session,
                step=step,
                run=run,
                task=task,
                category="project_pin_unresolved",
                quota=False,
                summary="Pinned project model is unavailable or its configuration changed.",
            )
            continue
        if matrix_choice is not None:
            pin_family_ids = frozenset({matrix_choice.profile_id})
            failed_profile_id = None
            allowed_fallbacks = ()
        elif pin_eligible_step and preference is not None:
            pin_preference_revision = preference.revision
            if not preference.is_auto:
                pin_worker_key = (
                    preference.worker_key.value if preference.worker_key is not None else None
                )
                project_pin = resolve_route_pin(preference, registries)
                if project_pin is None:
                    # Stored pin must not silently degrade to unrestricted auto routing.
                    await _persist_decision(
                        session,
                        run=run,
                        step=step,
                        role=role,
                        attempt=attempt,
                        selected_profile_id=None,
                        decision=PolicyDecision(
                            selected_profile_id=None,
                            alternatives=(),
                            evaluations=(),
                        ),
                        request=RoutingRequest(
                            role=role,
                            task_type=routing_task_type,
                            required_capabilities=required,
                            project_allowed_capabilities=project_caps,
                            budget_mode=BudgetMode(run.budget_mode),
                            estimated_input_tokens=estimated_input,
                            max_output_tokens=requested_output,
                            remaining_cost_units=settings.limits.task_cost_units,
                            estimated_cost_enforcement=settings.limits.estimated_cost_enforcement,
                        ),
                        pin_inputs={
                            "project_pin_mode": preference.mode.value,
                            "project_pin_worker": pin_worker_key,
                            "preference_revision": preference.revision,
                            "project_pin_unresolved": True,
                        },
                    )
                    await _block_route(
                        session,
                        step=step,
                        run=run,
                        task=task,
                        category="project_pin_unresolved",
                        quota=False,
                        summary=(
                            "Project /model pin cannot be resolved to an enabled executor profile."
                        ),
                    )
                    continue
                # Fence: only the pinned profile and same-family fallbacks, every attempt.
                same_family = (
                    same_family_fallback_ids(
                        registries,
                        worker_key=project_pin.worker_key,
                        primary_profile_id=project_pin.trusted_profile_id,
                    )
                    if project_pin.allow_same_family_fallbacks
                    else ()
                )
                pin_family_ids = frozenset((project_pin.trusted_profile_id, *same_family))
                if failed_profile_id is not None:
                    allowed_fallbacks = tuple(
                        profile_id
                        for profile_id in allowed_fallbacks
                        if profile_id in pin_family_ids
                    )
                    if not allowed_fallbacks:
                        # Configured fallbacks may all be outside an exact project pin.
                        # In that case the bounded retry must stay on the pinned profile
                        # instead of excluding every permitted route.
                        failed_profile_id = None
                else:
                    # Initial claim: prefer trusted, allow same-family only if explicit edges exist.
                    allowed_fallbacks = same_family
        trusted_profile_id = (
            chat_profile_id
            or (matrix_choice.profile_id if matrix_choice else None)
            or (project_pin.trusted_profile_id if project_pin is not None else None)
        )
        default_executor_id = settings.execution.default_executor_profile_id
        default_executor_applied = False
        if pin_eligible_step and trusted_profile_id is None and default_executor_id is not None:
            try:
                primary = registries.profiles.get(default_executor_id)
            except RegistryError:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="default_executor_unavailable",
                    quota=False,
                    summary="Configured default CLI executor is unavailable.",
                )
                continue
            # A worker default is separate from budget-mode model strength and
            # the chat/planner/reviewer. Never fall through to another family.
            pin_family_ids = frozenset(
                (
                    primary.id,
                    *(
                        candidate.id
                        for candidate in profiles
                        if candidate.id in primary.fallback_profile_ids
                        and candidate.provider == primary.provider
                        and candidate.launch_mode is LaunchMode.CLI
                        and ProviderRole.EXECUTOR in candidate.roles
                    ),
                )
            )
            trusted_profile_id = primary.id
            default_executor_applied = True
            allowed_fallbacks = tuple(
                value for value in allowed_fallbacks if value in pin_family_ids
            )
        policy_request = RoutingRequest(
            role=role,
            task_type=routing_task_type,
            required_capabilities=required,
            project_allowed_capabilities=project_caps,
            budget_mode=BudgetMode(run.budget_mode),
            estimated_input_tokens=estimated_input,
            max_output_tokens=requested_output,
            remaining_cost_units=settings.limits.task_cost_units,
            estimated_cost_enforcement=settings.limits.estimated_cost_enforcement,
            trusted_profile_id=trusted_profile_id,
            failed_profile_id=failed_profile_id,
            allowed_fallback_ids=allowed_fallbacks,
            restrict_to_profile_ids=pin_family_ids,
            requires_sandbox=(
                step.step_type in {"execute_code", "execute_agent"}
                or (matrix_profile is not None and matrix_profile.launch_mode is LaunchMode.CLI)
            ),
            required_launch_mode=(
                matrix_profile.launch_mode
                if matrix_profile is not None
                else LaunchMode.CLI
                if step.step_type in {"execute_code", "execute_agent"}
                else LaunchMode.API
            ),
        )
        decision = select_profile(policy_request, profiles, states)
        ordered = tuple(
            profile_id
            for profile_id in (decision.selected_profile_id, *decision.alternatives)
            if profile_id is not None and (pin_family_ids is None or profile_id in pin_family_ids)
        )
        if not ordered and _temporarily_unavailable(decision):
            continue
        selected: ProviderProfileConfig | None = None
        reservation = None
        money_cap_hit = False
        unknown_cost = False
        consent_required = False
        goal_budget_hit = False
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
                    accounting=accounting_for_profile(
                        profile,
                        purpose=purpose_for_step_type(step.step_type),
                        attempt_kind=attempt_kind_for_step(step),
                        # D3 lifetime owner (lead Q1): materialized tasks
                        # scope to their package; the row carries it and
                        # settle inherits it (sticky, no per-call plumbing).
                        horizon_id=await resolve_horizon_scope(session, task_id=task.id),
                    ),
                    admission_only=ceiling_is_admission_only(profile),
                )
            except BudgetExceeded as error:
                message = str(error)
                if isinstance(error, GoalBudgetExceeded):
                    goal_budget_hit = True
                    break
                if message.startswith("explicit money cap cannot cover"):
                    consent_required = True
                elif message.startswith("explicit money cap cannot be certified"):
                    unknown_cost = True
                elif message.startswith("explicit money cap"):
                    money_cap_hit = True
                continue
            selected = profile
            if settings.limits.explicit_money_cap_units is not None and ceiling_is_admission_only(
                profile
            ):
                await note_admission_only_ceiling(session, task.id, profile.id)
            break
        pin_decision_inputs = _pin_decision_inputs(
            project_pin=project_pin,
            pin_family_ids=pin_family_ids,
            preference_revision=pin_preference_revision,
            worker_key=pin_worker_key,
        )
        if matrix_choice is not None:
            pin_decision_inputs = dict(pin_decision_inputs or {})
            pin_decision_inputs.update(
                project_model_slot=matrix_slot,
                project_model_assignment=matrix_choice.model_dump(mode="json"),
            )
        if chat_profile_id is not None:
            pin_decision_inputs = dict(pin_decision_inputs or {})
            pin_decision_inputs.update(
                {
                    "chat_profile_id": chat_profile_id,
                    "chat_profile_revision": run.selected_route["profile_revision"],
                }
            )
        if default_executor_applied:
            pin_decision_inputs = dict(pin_decision_inputs or {})
            pin_decision_inputs["default_executor_profile_id"] = default_executor_id
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
                pin_inputs=pin_decision_inputs,
            )
            if goal_budget_hit:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="goal_budget_exhausted",
                    quota=False,
                    summary=(
                        "Бюджет цели исчерпан. Расходы и результат сохранены. "
                        "Увеличьте лимит через /lead limits cost или закройте цель."
                    ),
                )
            elif consent_required:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="admission_only_consent_required",
                    quota=False,
                    summary=ADMISSION_ONLY_CONSENT_SUMMARY,
                )
            elif unknown_cost:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="money_cap_uncertified",
                    quota=False,
                    summary=EXPLICIT_MONEY_CAP_UNKNOWN_SUMMARY,
                )
            elif money_cap_hit:
                await _block_route(
                    session,
                    step=step,
                    run=run,
                    task=task,
                    category="money_cap_reached",
                    quota=False,
                    summary=EXPLICIT_MONEY_CAP_EXCEEDED_SUMMARY,
                )
            else:
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
            pin_inputs=pin_decision_inputs,
        )
        step.executor_profile_id = selected.id
        step.status = StepStatus.LEASED
        step.lease_owner = owner
        step.lease_generation += 1
        step.heartbeat_at = func.now()
        step.lease_expires_at = func.now() + timedelta(seconds=lease_seconds)
        step.attempt_count += 1
        next_payload = {
            **step.payload,
            "provider_attempt": attempt,
            "budget_reservation_id": str(reservation.id),
        }
        if chat_profile_id is not None:
            from vuzol.projects.model_matrix import ModelAssignment

            # Carry the immutable identity through the host sandbox boundary as
            # well, using its existing profile revision fence (not live /model).
            next_payload.pop("executor_model_override", None)
            next_payload.pop("executor_reasoning_effort", None)
            next_payload.update(
                project_model_slot=run.selected_route.get("conversation_mode", "think"),
                project_model_assignment=ModelAssignment(
                    profile_id=selected.id,
                    model=selected.model,
                    reasoning_effort=selected.model_reasoning_effort,
                    content_revision=run.selected_route["profile_revision"],
                ).model_dump(mode="json"),
            )
        if matrix_choice is not None:
            # The configured profile already carries the exact snapshotted model
            # and effort. Older worker overrides must not replace that selection.
            next_payload.pop("executor_model_override", None)
            next_payload.pop("executor_reasoning_effort", None)
            next_payload.update(
                {
                    "project_model_slot": matrix_slot,
                    "project_model_assignment": matrix_choice.model_dump(mode="json"),
                }
            )
        if project_pin is not None and pin_family_ids is not None:
            if selected.id not in pin_family_ids:
                # Defensive: fence must make this unreachable; never mis-attribute.
                pass
            else:
                # Always keep pin model/effort overrides; record actual selected provenance.
                next_payload.update(preference_payload(project_pin))
                if selected.id != project_pin.trusted_profile_id:
                    next_payload["executor_fallback_profile_id"] = selected.id
                    next_payload["executor_pin_profile_id"] = project_pin.trusted_profile_id
        step.payload = next_payload
        await session.flush()
        await session.refresh(step, attribute_names=["heartbeat_at", "lease_expires_at"])
        if task.source_chat_id and task.task_type != "discussion_agent_internal":
            await enqueue_project_status_dashboard(session, task.source_chat_id)
        return LeaseToken(step=step_record(step), owner=owner, generation=step.lease_generation)
    return None


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


def _pin_decision_inputs(
    *,
    project_pin: ExecutorRoutePin | None,
    pin_family_ids: frozenset[str] | None,
    preference_revision: int | None,
    worker_key: str | None,
) -> dict[str, object] | None:
    """Bounded pin provenance stored on RoutingDecision.inputs."""

    if project_pin is None and pin_family_ids is None and preference_revision is None:
        return None
    payload: dict[str, object] = {}
    if preference_revision is not None:
        payload["preference_revision"] = preference_revision
    if worker_key is not None:
        payload["project_pin_worker"] = worker_key
    if project_pin is not None:
        payload["project_pin_mode"] = "pin"
        payload["trusted_profile_id"] = project_pin.trusted_profile_id
        if project_pin.model_override is not None:
            payload["model_override"] = project_pin.model_override
        if project_pin.reasoning_effort is not None:
            payload["reasoning_effort"] = project_pin.reasoning_effort
    if pin_family_ids is not None:
        payload["restrict_to_profile_ids"] = sorted(pin_family_ids)
    return payload or None


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
    pin_inputs: dict[str, object] | None = None,
) -> None:
    evaluations = [
        {
            "profile_id": evaluation.profile_id,
            "eligible": evaluation.eligible,
            "reasons": [reason.value for reason in evaluation.reasons],
        }
        for evaluation in decision.evaluations
    ]
    inputs: dict[str, object] = {
        "task_type": request.task_type,
        "required_capabilities": sorted(
            capability.value for capability in request.required_capabilities
        ),
        "budget_mode": request.budget_mode.value,
        "evaluations": evaluations,
    }
    if pin_inputs:
        inputs.update(pin_inputs)
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
            inputs=inputs,
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
    summary: str | None = None,
) -> None:
    if summary is None:
        summary = (
            "Внутренний бюджет этапа исчерпан. Сохранённый результат и расходы не потеряны; "
            "безопасный ручной повтор доступен через актуальную карточку задачи."
            if category == "budget_exhausted"
            else "Доступный лимит провайдера исчерпан; задача ожидает возобновления квоты."
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
    target_task_status = TaskStatus.QUOTA_EXHAUSTED if quota else TaskStatus.BLOCKED
    if task.status != target_task_status:
        await transition_task(
            session,
            task,
            target_task_status,
            actor_type="routing_policy",
            payload={"category": category},
        )
    await enqueue_terminal_task_projections(session, task, run)


def _estimated_input_tokens(task: Task, step: Step, run: Run) -> int:
    """Estimate the full provider input, not just the original text.

    Includes the task draft, the step's declared context-manifest estimate, the
    output JSON schema and the system/prompt revisions so the reservation is not
    systematically undersized (gap E21).
    """

    characters = len(task.original_text) + len(
        json.dumps(task.task_draft, ensure_ascii=False, sort_keys=True)
    )
    payload = step.payload if isinstance(step.payload, dict) else {}
    context_tokens = payload.get("context_estimate_tokens")
    if (
        isinstance(context_tokens, int)
        and not isinstance(context_tokens, bool)
        and context_tokens > 0
    ):
        characters += context_tokens * 4
    draft = task.task_draft
    if step.payload.get("result_kind") == "report":
        draft = {**draft, "report_contract": "task-report.v1"}
    _name, _version, schema = result_schema_for_step(step.step_type, draft)
    characters += schema_char_count(schema)
    characters += len(run.policy_revision) + len(run.prompt_revision or "")
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
