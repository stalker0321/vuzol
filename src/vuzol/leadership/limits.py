"""Owner-specified work ceilings, enforced under the existing goal row lock."""

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.discussion.domain import DomainError
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import WorkPackage


class WorkLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    max_packages: int | None = Field(default=None, ge=1)
    max_items_per_package: int | None = Field(default=None, ge=1)


def parse_work_limits(raw: object) -> WorkLimits:
    try:
        return WorkLimits.model_validate(raw)
    except ValidationError as error:
        raise DomainError("invalid_work_limits") from error


async def check_batch_limits(session: AsyncSession, goal: ManagedGoal, *, items: int) -> None:
    limits = parse_work_limits(goal.constraints.get("work_limits", {}))
    if limits.max_items_per_package is not None and items > limits.max_items_per_package:
        raise DomainError("work_item_limit_reached")
    if limits.max_packages is not None:
        # Count all admitted packages, not only successful ones. Stopping/failing a
        # package or restarting the worker must not refund the owner's grant.
        used = await session.scalar(
            select(func.count())
            .select_from(WorkPackage)
            .where(WorkPackage.managed_goal_id == goal.id)
        )
        if int(used or 0) >= limits.max_packages:
            raise DomainError("work_package_limit_reached")
