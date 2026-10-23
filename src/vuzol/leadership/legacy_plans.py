"""Lazy data conversion of untouched pre-PR5 plan proposals; never rewrite history."""

from __future__ import annotations

from pathlib import Path

from vuzol.discussion.domain import DomainError
from vuzol.discussion.service import WorkPackageService
from vuzol.execution.git import GitError, LocalGit
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import WorkPackage
from vuzol.storage.types import PlanRevisionCreatedBy, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork


async def convert_legacy_plan(
    uow: UnitOfWork, goal: ManagedGoal, repository_path: Path | None
) -> None:
    """Caller holds the goal lock. Pointer + marker commit with the draft.

    NULL source_repo_commit explicitly records that legacy planning did not pin
    a repository. No old approval is transferred. Paused goals stay paused.
    """
    key = "legacy_plan_revision_v1"
    if goal.lifecycle in {"completed", "cancelled"} or key in goal.constraints:
        return
    raw = goal.proposed_plan
    if not isinstance(raw, dict):
        return
    assert uow.session is not None
    current = (
        await uow.session.get(WorkPackage, goal.current_package_id)
        if goal.current_package_id
        else None
    )
    if current is not None and current.status not in {
        WorkPackageStatus.COMPLETED,
        WorkPackageStatus.STOPPED,
        WorkPackageStatus.DISCARDED,
    }:
        return
    from vuzol.leadership.service import plan_draft_from_payload

    try:
        items = raw.get("items", [])
        plan = plan_draft_from_payload(
            {
                "plan": {
                    **raw,
                    "items": [
                        {
                            **item,
                            "local_id": item.get("local_id") or f"legacy-{ordinal}",
                            "dependencies": item.get("dependencies", []),
                        }
                        for ordinal, item in enumerate(items, 1)
                    ],
                }
            }
        )
    except (DomainError, TypeError, AttributeError):
        # Retain the original data for inspection, but never execute an invalid
        # graph. A subsequent proposal uses the normal validated revision path.
        goal.constraints = {
            **goal.constraints,
            key: {"status": "invalid", "scope_revision": goal.scope_revision},
        }
        return
    service = WorkPackageService(uow)
    if await service.is_duplicate_terminal_plan(
        session_id=goal.discussion_session_id, project_id=goal.project_id, plan=plan
    ):
        goal.constraints = {**goal.constraints, key: {"status": "historical"}}
        return
    if await uow.work_packages.active_package_id(session_id=goal.discussion_session_id) is not None:
        return
    commit = None
    if repository_path is not None:
        try:
            commit = await LocalGit().read_ref(repository_path, goal.integration_branch)
            if commit is None:
                commit = await LocalGit().read_ref(
                    repository_path, str(goal.constraints.get("project_default_branch", "main"))
                )
        except (OSError, GitError):
            commit = None
    result = await WorkPackageService(uow).create_draft(
        session_id=goal.discussion_session_id,
        project_id=goal.project_id,
        plan=plan,
        created_by=PlanRevisionCreatedBy.SYSTEM,
        actor_type="system",
        managed_goal_id=goal.id,
        source_repo_commit=commit,
    )
    goal.version += 1
    goal.fence_epoch += 1
    goal.current_package_id = result.package_id
    goal.constraints = {
        **goal.constraints,
        key: {
            "idempotency_key": f"lead:legacy-plan:{goal.id}:{goal.scope_revision}",
            "revision_id": str(result.revision_id),
            "scope_revision": goal.scope_revision,
        },
    }
    await WorkPackageService(uow)._enqueue_plan_projection(
        result.package_id, result.status_generation, "legacy_conversion"
    )
