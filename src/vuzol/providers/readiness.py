"""Static preflight for an explicitly assigned executor, before workflow work.

Health, concurrency and budget are deliberately checked later at claim time.
This check never switches provider families or creates a reservation.
"""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import ConfigurationBundle, Settings
from vuzol.config.models import BudgetMode, LaunchMode, ProviderRole
from vuzol.projects.executor_preference import (
    load_preference,
    resolve_route_pin,
    same_family_fallback_ids,
)
from vuzol.providers.policy import RoutingRequest, select_profile
from vuzol.workflows.domain import MaterializedWorkflow


@dataclass(frozen=True, slots=True)
class ExecutorReadinessFailure:
    step_type: str
    summary: str
    evaluations: tuple[dict[str, object], ...]


async def configured_executor_preflight(
    session: AsyncSession,
    *,
    settings: Settings,
    registries: ConfigurationBundle,
    project_id: str | None,
    task_type: str,
    workflow: MaterializedWorkflow,
) -> ExecutorReadinessFailure | None:
    steps = tuple(s for s in workflow.steps if s.step_type in {"execute_agent", "execute_code"})
    if not steps:
        return None
    profiles = registries.profiles.items()
    assigned = settings.execution.default_executor_profile_id
    family: frozenset[str] | None = None
    if project_id is not None:
        preference = await load_preference(session, project_id)
        if not preference.is_auto:
            pin = resolve_route_pin(preference, registries)
            if pin is None:
                return ExecutorReadinessFailure(
                    steps[0].step_type,
                    "Выбранный исполнитель проекта недоступен в конфигурации. "
                    "Работа не запускалась; проверь выбор модели проекта.",
                    (),
                )
            assigned = pin.trusted_profile_id
            family = frozenset(
                (
                    assigned,
                    *(
                        same_family_fallback_ids(
                            registries, worker_key=pin.worker_key, primary_profile_id=assigned
                        )
                        if pin.allow_same_family_fallbacks
                        else ()
                    ),
                )
            )
    if assigned is None:
        # Unpinned auto routing has no assigned family to attest here.
        return None
    if family is None:
        primary = next((p for p in profiles if p.id == assigned), None)
        family = frozenset(
            (
                assigned,
                *(
                    p.id
                    for p in profiles
                    if primary is not None
                    and p.id in primary.fallback_profile_ids
                    and p.provider == primary.provider
                    and p.launch_mode is LaunchMode.CLI
                    and ProviderRole.EXECUTOR in p.roles
                ),
            )
        )
    project_caps = registries.projects.get(project_id).allowed_capabilities if project_id else None
    for step in steps:
        decision = select_profile(
            RoutingRequest(
                role=ProviderRole.EXECUTOR,
                task_type=task_type,
                required_capabilities=step.capabilities,
                project_allowed_capabilities=project_caps,
                budget_mode=BudgetMode.BALANCED,
                estimated_input_tokens=0,
                max_output_tokens=0,
                remaining_cost_units=float("inf"),
                trusted_profile_id=assigned,
                restrict_to_profile_ids=family,
                requires_sandbox=True,
                required_launch_mode=LaunchMode.CLI,
            ),
            profiles,
            {},
        )
        if decision.selected_profile_id is None:
            return ExecutorReadinessFailure(
                step.step_type,
                f"Назначенный исполнитель {assigned} и его разрешённые резервы не поддерживают "  # noqa: RUF001
                f"сценарий {task_type} с требуемыми правами. Работа не запускалась. "  # noqa: RUF001
                "Нужно исправить конфигурацию маршрута или явно выбрать совместимого исполнителя.",
                tuple(
                    {"profile_id": e.profile_id, "reasons": [r.value for r in e.reasons]}
                    for e in decision.evaluations
                    if e.profile_id in family
                ),
            )
    return None
