"""Goal completion reads applied package evidence. It never writes PASS."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import PlanRevision, PlanRevisionItem, WorkPackage
from vuzol.storage.types import PlanRevisionState, WorkPackageStatus


@dataclass(frozen=True, slots=True)
class CompletionAssessment:
    satisfied: bool
    covered: tuple[str, ...]
    missing: tuple[str, ...]


async def assess_completion(session: AsyncSession, goal: ManagedGoal) -> CompletionAssessment:
    """A criterion counts only on an applied managed-branch head."""

    wanted = [
        str(item["id"]) for item in goal.criteria if isinstance(item, dict) and item.get("id")
    ]
    covered: set[str] = set()
    packages = (
        await session.scalars(
            select(WorkPackage).where(
                WorkPackage.managed_goal_id == goal.id,
                WorkPackage.status == WorkPackageStatus.COMPLETED,
            )
        )
    ).all()
    for package in packages:
        head = package.integration_head_commit
        base = package.integration_base_commit
        if not head or head == base or package.integration_branch != goal.integration_branch:
            continue
        revision_id = package.running_revision_id or package.approved_revision_id
        if revision_id is None:
            continue
        revision = await session.get(PlanRevision, revision_id)
        if revision is None or revision.state is not PlanRevisionState.APPROVED:
            continue
        items = (
            await session.scalars(
                select(PlanRevisionItem).where(PlanRevisionItem.plan_revision_id == revision.id)
            )
        ).all()
        for item in items:
            covered.update(str(value) for value in item.completion_criteria)
    present = tuple(criterion_id for criterion_id in wanted if criterion_id in covered)
    missing = tuple(criterion_id for criterion_id in wanted if criterion_id not in covered)
    return CompletionAssessment(
        satisfied=bool(wanted) and not missing,
        covered=present,
        missing=missing,
    )


def evidence_token(package: WorkPackage | None) -> str:
    """Identity of the package head. A stop does not change it; an apply does."""

    if package is None:
        return "none"
    return f"{package.id}:{package.integration_head_commit}"


async def package_has_unknown_effects(session: AsyncSession, package_id: uuid.UUID) -> bool:
    from vuzol.storage.models import MaterializationLink, Run, Step

    found = await session.scalar(
        select(Step.id)
        .join(Run, Run.id == Step.run_id)
        .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
        .where(
            MaterializationLink.work_package_id == package_id,
            Step.unknown_effects.is_(True),
        )
        .limit(1)
    )
    return found is not None
