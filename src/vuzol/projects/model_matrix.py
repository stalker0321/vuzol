"""Authorized project role assignments, pinned by complete profile revision."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.revision import content_revision
from vuzol.projects.executor_preference import (
    ExecutorPreferenceError,
    ExecutorPreferenceMode,
    ExecutorPreferenceView,
    ExecutorWorkerKey,
    ensure_preference_row,
)
from vuzol.providers.catalog import supported_profile

MATRIX_KEY = "project_model_assignments"
LEGACY_EXECUTOR_KEY = "project_executor_preference"
SLOT_ROLES = {
    "lead": ProviderRole.LEAD,
    "goal_reviewer": ProviderRole.REVIEWER,
    "planner_light": ProviderRole.PLANNER,
    "planner_strong": ProviderRole.PLANNER,
    "reviewer_light": ProviderRole.REVIEWER,
    "reviewer_strong": ProviderRole.REVIEWER,
    "executor": ProviderRole.EXECUTOR,
}
SLOT_LABELS = {
    "lead": "Ведущий",
    "goal_reviewer": "Проверка цели",
    "planner_light": "Планировщик · light",
    "planner_strong": "Планировщик · strong",
    "reviewer_light": "Ревьювер · light",
    "reviewer_strong": "Ревьювер · strong",
    "executor": "Исполнитель",
}


class ModelAssignment(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,99}$")
    model: str = Field(min_length=1, pattern=r"^[^\x00-\x1f\x7f]+$")
    reasoning_effort: str | None = Field(
        default=None, pattern=r"^(low|medium|high|xhigh|max|ultra)$"
    )
    content_revision: str = Field(pattern=r"^[0-9a-f]{64}$")


def slot_for_role(role: ProviderRole, *, strong: bool) -> str | None:
    if role is ProviderRole.EXECUTOR:
        return "executor"
    if role in {ProviderRole.PLANNER, ProviderRole.REVIEWER}:
        return f"{role.value}_{'strong' if strong else 'light'}"
    return None


def eligible_profiles(
    registries: ConfigurationBundle, slot: str
) -> tuple[ProviderProfileConfig, ...]:
    if slot not in SLOT_ROLES:
        raise ExecutorPreferenceError("unknown model role")
    role = SLOT_ROLES[slot]
    return tuple(
        sorted(
            (
                profile
                for profile in registries.profiles.items()
                if profile.enabled
                and supported_profile(profile)
                and role in profile.roles
                and (role is not ProviderRole.EXECUTOR or profile.launch_mode is LaunchMode.CLI)
            ),
            key=lambda profile: (profile.routing_priority, profile.id),
        )
    )


def assignment_snapshot(value: object) -> dict[str, Any]:
    """Validate and copy JSON so later preference edits cannot mutate a run."""
    if not isinstance(value, dict) or any(slot not in SLOT_ROLES for slot in value):
        raise ValueError("invalid project model snapshot")
    return {
        slot: ModelAssignment.model_validate(raw).model_dump(mode="json")
        for slot, raw in value.items()
    }


def pinned_assignment(snapshot: dict[str, Any], slot: str | None) -> ModelAssignment | None:
    if slot is None:
        return None
    if slot not in SLOT_ROLES:
        raise ValueError("unknown model role")
    if not isinstance(snapshot, dict):
        raise ValueError("invalid project model snapshot")
    assignments = assignment_snapshot(snapshot.get(MATRIX_KEY, {}))
    return ModelAssignment.model_validate(assignments[slot]) if slot in assignments else None


def assignment_profile(
    assignment: ModelAssignment,
    registries: ConfigurationBundle,
    slot: str,
) -> ProviderProfileConfig:
    profile = next(
        (
            profile
            for profile in eligible_profiles(registries, slot)
            if profile.id == assignment.profile_id
        ),
        None,
    )
    if profile is None:
        raise ExecutorPreferenceError("selected model profile is unavailable; send /model")
    if (
        content_revision(profile) != assignment.content_revision
        or profile.model != assignment.model
        or profile.model_reasoning_effort != assignment.reasoning_effort
    ):
        raise ExecutorPreferenceError("selected model profile configuration changed; send /model")
    return profile


def legacy_executor_preference(snapshot: dict[str, Any]) -> ExecutorPreferenceView | None:
    """Read only the frozen compatibility choice for a pre-matrix executor pin."""
    raw = snapshot.get(LEGACY_EXECUTOR_KEY)
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("invalid executor preference snapshot")
    return ExecutorPreferenceView(
        project_id=raw["project_id"],
        mode=ExecutorPreferenceMode(raw["mode"]),
        worker_key=ExecutorWorkerKey(raw["worker_key"]) if raw["worker_key"] else None,
        profile_id=raw.get("profile_id"),
        reasoning_effort=raw["reasoning_effort"],
        revision=raw["revision"],
    )


async def set_assignment(
    session: AsyncSession,
    *,
    project_id: str,
    slot: str,
    profile_id: str | None,
    expected_revision: int,
    user_id: int,
    registries: ConfigurationBundle,
) -> None:
    chosen = next(
        (profile for profile in eligible_profiles(registries, slot) if profile.id == profile_id),
        None,
    )
    if profile_id is not None and chosen is None:
        raise ExecutorPreferenceError("selected model profile is unavailable; send /model")
    row = await ensure_preference_row(session, project_id)
    if row.revision != expected_revision:
        raise ExecutorPreferenceError("model options are stale; send /model again")
    assignments = assignment_snapshot(row.model_assignments or {})
    if chosen is None:
        assignments.pop(slot, None)
    else:
        assignments[slot] = ModelAssignment(
            profile_id=chosen.id,
            model=chosen.model,
            reasoning_effort=chosen.model_reasoning_effort,
            content_revision=content_revision(chosen),
        ).model_dump(mode="json")
    row.model_assignments = assignments
    if slot == "executor":
        # Clearing a matrix choice must not revive an older worker preference.
        row.mode = "auto"
        row.worker_key = None
        row.profile_id = None
        row.reasoning_effort = None
    row.revision += 1
    row.updated_by_user_id = user_id
    await session.flush()
