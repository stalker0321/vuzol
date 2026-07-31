import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from vuzol.storage.models import (
    EditSession,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    Task,
    WorkItemDraft,
    WorkPackage,
    WorkPackageOpenDetail,
)
from vuzol.storage.types import (
    EditSessionStatus,
    EstimatedComplexity,
    PlanRevisionCreatedBy,
    PlanRevisionState,
    RiskLevel,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork

from .helpers import storage

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


async def test_revision_items_keep_stable_identity_and_fenced_detail_without_tasks(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    first_hash = _hash("revision-1")
    second_hash = _hash("revision-2")

    async with UnitOfWork(factory) as uow:
        discussion_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=81
        )
        package = WorkPackage(
            session_id=discussion_id,
            project_id="vuzol",
            status=WorkPackageStatus.DRAFT,
            title="Первый пакет работ",
        )
        package_id = await uow.work_packages.add_package(package)

        first_revision = PlanRevision(
            work_package_id=package_id,
            revision_number=1,
            state=PlanRevisionState.SUPERSEDED,
            content_hash=first_hash,
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            planner_profile="planner-test",
            prompt_version="work-package-v1",
            immutable_body={"title": "Первый пакет работ", "revision": 1},
        )
        first_revision_id = await uow.work_packages.add_revision(first_revision)

        first_item = WorkItemDraft(work_package_id=package_id, local_id="storage")
        second_item = WorkItemDraft(work_package_id=package_id, local_id="domain")
        first_item_id = await uow.work_packages.add_item(first_item)
        second_item_id = await uow.work_packages.add_item(second_item)

        for ordinal, item, summary in (
            (1, first_item_id, "Добавить хранение"),
            (2, second_item_id, "Добавить домен"),
        ):
            await uow.work_packages.add_revision_item(
                PlanRevisionItem(
                    work_package_id=package_id,
                    plan_revision_id=first_revision_id,
                    item_id=item,
                    ordinal=ordinal,
                    summary=summary,
                    goal="Сохранить устойчивую идентичность.",
                    expected_outcome="Идентичность доступна после перечитывания.",
                    completion_criteria=["Строка сохранена", "Связи валидны"],
                    allowed_scope="src/vuzol/storage/**",
                    out_of_scope=["Telegram ingress"],
                    dependencies=[],
                    trusted_checks=["pytest"],
                    suggested_risk=RiskLevel.LOW,
                    needs_approval=False,
                    estimated_complexity=EstimatedComplexity.MEDIUM,
                )
            )

        second_revision = PlanRevision(
            work_package_id=package_id,
            revision_number=2,
            parent_revision_id=first_revision_id,
            state=PlanRevisionState.DRAFT,
            content_hash=second_hash,
            created_by=PlanRevisionCreatedBy.USER,
            immutable_body={"title": "Первый пакет работ", "revision": 2},
        )
        second_revision_id = await uow.work_packages.add_revision(second_revision)
        second_revision_memberships: dict[str, uuid.UUID] = {}
        for ordinal, item, summary in (
            (1, second_item_id, "Добавить домен"),
            (2, first_item_id, "Добавить хранение"),
        ):
            membership = PlanRevisionItem(
                work_package_id=package_id,
                plan_revision_id=second_revision_id,
                item_id=item,
                ordinal=ordinal,
                summary=summary,
                goal="Проверить copy-on-write.",
                expected_outcome="Новая ревизия не меняет стабильный item id.",
                completion_criteria=["Порядок изменён"],
                allowed_scope="src/vuzol/storage/**",
                suggested_risk=RiskLevel.LOW,
                needs_approval=False,
                estimated_complexity=EstimatedComplexity.SMALL,
            )
            await uow.work_packages.add_revision_item(membership)
            second_revision_memberships[summary] = membership.id

        package.head_revision_id = second_revision_id
        session = uow.session
        assert session is not None
        discussion = await session.get(ProjectDiscussionSession, discussion_id)
        assert discussion is not None
        discussion.active_work_package_id = package_id
        edit_session = EditSession(
            package_id=package_id,
            plan_revision_id=second_revision_id,
            plan_revision_number=2,
            content_hash=second_hash,
            item_id=first_item_id,
            ordinal=2,
            opened_by_user_id=42,
            status=EditSessionStatus.OPEN,
            session_generation=1,
            expires_at=datetime.now(UTC) + timedelta(minutes=15),
        )
        edit_session_id = await uow.work_packages.add_edit_session(edit_session)
        await uow.work_packages.set_open_detail(
            package_id=package_id,
            plan_revision_id=second_revision_id,
            item_id=first_item_id,
            plan_revision_number=2,
            h8=second_hash[:8],
            ordinal=2,
        )

    async with UnitOfWork(factory) as uow:
        resolved = await uow.work_packages.resolve_fenced_item(
            package_id=package_id,
            revision_number=2,
            h8=second_hash[:8],
            ordinal=2,
        )
    async with factory() as session:
        memberships = (
            await session.scalars(
                select(PlanRevisionItem)
                .where(PlanRevisionItem.item_id == first_item_id)
                .order_by(PlanRevisionItem.plan_revision_id)
            )
        ).all()
        detail = await session.get(WorkPackageOpenDetail, package_id)
        persisted_edit = await session.get(EditSession, edit_session_id)
        task_count = await session.scalar(select(func.count()).select_from(Task))
        link_count = await session.scalar(select(func.count()).select_from(MaterializationLink))

    assert len(memberships) == 2
    assert {membership.ordinal for membership in memberships} == {1, 2}
    assert resolved == (second_revision_id, first_item_id)
    assert detail is not None and detail.item_id == first_item_id and detail.ordinal == 2
    assert persisted_edit is not None and persisted_edit.session_generation == 1
    assert task_count == 0
    assert link_count == 0

    async with UnitOfWork(factory) as uow:
        canonical_task = await uow.tasks.create(
            user_id=42,
            chat_id=-1001,
            thread_id=81,
            project_id="vuzol",
            original_text="Выполнить выбранный пункт плана.",
            task_type="coding",
        )
        link_id = await uow.work_packages.add_materialization(
            MaterializationLink(
                work_package_id=package_id,
                plan_revision_id=second_revision_id,
                work_item_draft_id=first_item_id,
                plan_revision_item_id=second_revision_memberships["Добавить хранение"],
                task_id=canonical_task.id,
                ordinal=2,
            )
        )
    async with factory() as session:
        link = await session.get(MaterializationLink, link_id)
    assert link is not None and link.task_id == canonical_task.id

    with pytest.raises(ValueError, match="open-detail fence"):
        async with UnitOfWork(factory) as uow:
            await uow.work_packages.set_open_detail(
                package_id=package_id,
                plan_revision_id=second_revision_id,
                item_id=first_item_id,
                plan_revision_number=2,
                h8="00000000",
                ordinal=2,
            )
    await engine.dispose()


async def test_edit_session_rejects_cross_package_revision_item_identity(
    postgres_dsn: str,
) -> None:
    engine, factory = storage(postgres_dsn)
    content_hash = _hash("cross-package")
    async with UnitOfWork(factory) as uow:
        first_discussion = await uow.discussions.create_session(
            project_id="first", chat_id=-1001, message_thread_id=82
        )
        second_discussion = await uow.discussions.create_session(
            project_id="second", chat_id=-1001, message_thread_id=83
        )
        first_package = WorkPackage(session_id=first_discussion, project_id="first", title="Первый")
        second_package = WorkPackage(
            session_id=second_discussion, project_id="second", title="Второй"
        )
        first_package_id = await uow.work_packages.add_package(first_package)
        second_package_id = await uow.work_packages.add_package(second_package)
        revision = PlanRevision(
            work_package_id=first_package_id,
            revision_number=1,
            state=PlanRevisionState.DRAFT,
            content_hash=content_hash,
            created_by=PlanRevisionCreatedBy.USER,
            immutable_body={"revision": 1},
        )
        revision_id = await uow.work_packages.add_revision(revision)
        item = WorkItemDraft(work_package_id=first_package_id)
        item_id = await uow.work_packages.add_item(item)
        await uow.work_packages.add_revision_item(
            PlanRevisionItem(
                work_package_id=first_package_id,
                plan_revision_id=revision_id,
                item_id=item_id,
                ordinal=1,
                summary="Проверить fence",
                goal="Пакеты должны оставаться раздельными.",
                expected_outcome="База отклоняет чужую идентичность.",
                completion_criteria=["FK отклоняет запись"],
                allowed_scope="storage",
                suggested_risk=RiskLevel.LOW,
                needs_approval=False,
                estimated_complexity=EstimatedComplexity.SMALL,
            )
        )

    with pytest.raises(ValueError, match="edit-session fence"):
        async with UnitOfWork(factory) as uow:
            await uow.work_packages.add_edit_session(
                EditSession(
                    package_id=second_package_id,
                    plan_revision_id=revision_id,
                    plan_revision_number=1,
                    content_hash=content_hash,
                    item_id=item_id,
                    ordinal=1,
                    opened_by_user_id=42,
                    status=EditSessionStatus.OPEN,
                    session_generation=1,
                    expires_at=datetime.now(UTC) + timedelta(minutes=15),
                )
            )
    await engine.dispose()
