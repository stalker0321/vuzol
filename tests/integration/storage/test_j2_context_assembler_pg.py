"""J2 context assembler integration against existing state owners."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from vuzol.context.assembler import AssemblerError
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.discussion.context_assembler import (
    assemble_pending_interactions,
    assemble_profile_slice,
    consume_pending_edit,
)
from vuzol.storage.models import AcceptedDecision, EditSession, WorkPackage
from vuzol.storage.types import (
    AcceptedDecisionStatus,
    EditSessionStatus,
    PlanRevisionCreatedBy,
)
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def plan(title: str = "J2 package", *, item_id: uuid.UUID | None = None) -> PlanDraft:
    return PlanDraft(
        title=title,
        items=(
            PlanItemDraft(
                item_id=item_id,
                local_id="domain",
                summary="Implement lifecycle",
                goal="Persist fenced work-package changes",
                expected_outcome="A deterministic revision exists",
                completion_criteria=("Domain tests pass",),
                allowed_scope="src/vuzol/discussion/**",
            ),
        ),
    )


async def _create_package(factory: object) -> tuple[uuid.UUID, uuid.UUID, str]:
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=91
        )
        result = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
    return session_id, result.package_id, result.content_hash


async def test_multiple_pending_sessions_require_resolution_and_consume_once(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    _, package_id, content_hash = await _create_package(factory)

    async with UnitOfWork(factory) as uow:
        service = WorkPackageService(uow)
        first = await service.open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=42,
        )
        second = await service.open_edit_session(
            package_id=package_id,
            revision_number=1,
            h8=content_hash[:8],
            ordinal=1,
            user_id=7,
        )
        head = await uow.work_packages.get_revision(first.plan_revision_id)
        item_id = uuid.UUID(str(head.immutable_body["items"][0]["item_id"]))

    async with factory() as session:
        pending = await assemble_pending_interactions(session, package_id=package_id)
    assert pending.requires_resolution is True
    assert len(pending.interactions) == 2
    first_projection = next(
        projection for projection in pending.interactions if projection.interaction_id == first.id
    )
    assert first_projection.delivered_options[0].option_id == "domain"

    # A foreign principal is rejected before the domain service is touched.
    with pytest.raises(AssemblerError) as foreign:
        async with UnitOfWork(factory) as uow:
            await consume_pending_edit(
                uow,
                projection=first_projection,
                expected_version=1,
                replacement=plan("Foreign edit", item_id=item_id),
                user_id=7,
                project_id="vuzol",
            )
    assert foreign.value.category == "foreign_principal"

    async with UnitOfWork(factory) as uow:
        result = await consume_pending_edit(
            uow,
            projection=first_projection,
            expected_version=1,
            replacement=plan("Edited package", item_id=item_id),
            user_id=42,
            project_id="vuzol",
        )
    assert result.revision_number == 2

    async with factory() as session:
        persisted = await session.get(EditSession, first.id)
        stale = await session.get(EditSession, second.id)
        remaining = await assemble_pending_interactions(session, package_id=package_id)
    assert persisted is not None and persisted.status is EditSessionStatus.ACCEPTED
    # Creating a new revision invalidates every other open session for the
    # package: the stale option is closed by the domain, not applied.
    assert stale is not None and stale.status is EditSessionStatus.CLOSED
    assert remaining.interactions == ()
    assert remaining.requires_resolution is False
    await engine.dispose()


async def test_profile_slice_invalidated_by_accepted_constraint_change(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    session_id, _, _ = await _create_package(factory)

    async with factory.begin() as session:
        decision = AcceptedDecision(
            session_id=session_id,
            key="database",
            statement="Use PostgreSQL",
            accepted_by_user_id=42,
            status=AcceptedDecisionStatus.ACTIVE,
        )
        session.add(decision)

    async with factory() as session:
        first = await assemble_profile_slice(session, session_id=session_id)
    assert [fact.key for fact in first.facts] == ["database"]
    assert first.is_current(first.accepted_revision_hash)

    async with factory.begin() as session:
        stored = await session.scalar(
            select(AcceptedDecision).where(AcceptedDecision.session_id == session_id)
        )
        assert stored is not None
        stored.statement = "Use PostgreSQL 16"

    async with factory() as session:
        second = await assemble_profile_slice(session, session_id=session_id)
    assert second.accepted_revision_hash != first.accepted_revision_hash
    assert first.is_current(second.accepted_revision_hash) is False

    async with factory() as session:
        package = await session.scalar(
            select(WorkPackage).where(WorkPackage.session_id == session_id)
        )
    assert package is not None
    await engine.dispose()
