from __future__ import annotations

import pytest
from sqlalchemy import func, select, update

from tests.integration.storage.helpers import storage
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.storage.models import PlanRevision, Task, WorkPackageOpenDetail
from vuzol.storage.types import PlanRevisionCreatedBy
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.work_package_projections import (
    WorkPackageProjectionError,
    build_work_package_detail_card,
    build_work_package_plan_card,
)
from vuzol.telegram.work_packages import parse_work_package_callback

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _plan() -> PlanDraft:
    return PlanDraft(
        title="Plan <unsafe>",
        items=tuple(
            PlanItemDraft(
                local_id=f"item-{ordinal}",
                summary=f"Step <{ordinal}>",
                goal=f"Goal {ordinal}",
                expected_outcome=f"Outcome {ordinal}",
                completion_criteria=(f"Check {ordinal}",),
                allowed_scope="src/**",
            )
            for ordinal in range(1, 11)
        ),
    )


async def _create(factory: object) -> tuple[object, object]:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=91
        )
        result = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
    return result.package_id, result


async def test_plan_projection_is_pg_reconstructable_and_fenced(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    package_id, result = await _create(factory)

    async with factory() as session:
        first = await build_work_package_plan_card(session, package_id, page=1)  # type: ignore[arg-type]
        second = await build_work_package_plan_card(session, package_id, page=2)  # type: ignore[arg-type]

    assert "Plan &lt;unsafe&gt;" in first.html and "Step &lt;1&gt;" in first.html
    assert "Step &lt;9&gt;" in second.html and first.status_generation == 1
    callbacks = [
        parse_work_package_callback(data) for row in first.callback_buttons for _, data in row
    ]
    assert all(callback.package_id == package_id for callback in callbacks)
    assert all(callback.revision_number == 1 for callback in callbacks)
    assert all(callback.h8 == result.content_hash[:8] for callback in callbacks)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    await engine.dispose()


async def test_detail_projection_uses_stable_fenced_pointer(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    package_id, result = await _create(factory)
    async with UnitOfWork(factory) as uow:
        await WorkPackageService(uow).set_open_detail(
            package_id=package_id,
            revision_number=1,
            h8=result.content_hash[:8],
            ordinal=2,
        )
    async with factory() as session:
        detail = await build_work_package_detail_card(session, package_id)  # type: ignore[arg-type]
    assert detail is not None
    assert "2. Step &lt;2&gt;" in detail.html
    assert {label for row in detail.callback_buttons for label, _ in row} == {"Изменить", "Закрыть"}

    async with factory.begin() as session:
        await session.execute(
            update(WorkPackageOpenDetail)
            .where(WorkPackageOpenDetail.package_id == package_id)
            .values(h8="00000000")
        )
    async with factory() as session:
        with pytest.raises(WorkPackageProjectionError, match="stale_detail_pointer"):
            await build_work_package_detail_card(session, package_id)  # type: ignore[arg-type]
    await engine.dispose()


async def test_missing_detail_reconstructs_as_clear(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    package_id, _ = await _create(factory)
    async with factory() as session:
        assert await build_work_package_detail_card(session, package_id) is None  # type: ignore[arg-type]
        revision_count = await session.scalar(select(func.count()).select_from(PlanRevision))
    assert revision_count == 1
    await engine.dispose()
