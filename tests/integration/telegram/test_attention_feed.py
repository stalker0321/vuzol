"""Durable attention history through actual state producers and Telegram delivery."""
# ruff: noqa: RUF001

import uuid

import pytest
from sqlalchemy import select

from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    THREAD,
    _enable,
    _factory,
    _lead_service,
    _session,
)
from tests.integration.leadership.test_plan_revisions import plan_items
from tests.integration.leadership.test_task_control import command
from vuzol.config import ProjectConfig, TopicConfig, TopicKind, TopicRegistry
from vuzol.config.registries import ProjectRegistry
from vuzol.discussion.service import WorkPackageService
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.persistence import ManagedGoal
from vuzol.leadership.planning import publish_outcome
from vuzol.leadership.service import enqueue_goal_status
from vuzol.storage.models import (
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ProjectDiscussionSession,
    Task,
    TelegramMessageLink,
    TransactionalOutbox,
    WorkPackage,
)
from vuzol.storage.types import DeliveryStatus, TaskStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.attention import sync_attention, sync_goal_attention
from vuzol.telegram.delivery import DeliveryAction, TelegramDeliveryService, prepare_delivery
from vuzol.telegram.projections import FakeTelegramClient, enqueue_chat_notice
from vuzol.workflows.application import Principal
from vuzol.workflows.lead_events import publish_task_event

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]
CHAT = -1001234567890


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def topics(tmp_path):
    (tmp_path / "repository").mkdir()
    projects = ProjectRegistry(
        (
            ProjectConfig(
                id=PROJECT,
                display_name="Школа",
                repository_path=tmp_path / "repository",
                default_branch="main",
                allowed_capabilities=frozenset(),
                sandbox_profile="fixture",
            ),
        ),
        repository_root=tmp_path,
    )
    return TopicRegistry(
        (
            TopicConfig(
                chat_id=CHAT,
                message_thread_id=THREAD,
                kind=TopicKind.PROJECT,
                display_name="Сайт школы",
                project_id=PROJECT,
                default_workflow="coding_task",
            ),
            TopicConfig(
                chat_id=CHAT,
                message_thread_id=7,
                kind=TopicKind.APPROVALS,
                default_workflow="simple_model_task",
            ),
        ),
        projects=projects,
    )


async def seed(factory):
    goal_id = await _enable(factory, await _session(factory))
    async with factory.begin() as session:
        goal = await session.get(ManagedGoal, goal_id)
        goal.source_chat_id = CHAT
        discussion = await session.get(ProjectDiscussionSession, goal.discussion_session_id)
        discussion.chat_id = CHAT
        for row in await session.scalars(
            select(TransactionalOutbox).where(
                TransactionalOutbox.destination == "telegram",
            )
        ):
            row.payload = {**row.payload, "chat_id": CHAT}
    return goal_id


def delivery(factory, client, topics):
    return TelegramDeliveryService(
        factory,
        client,
        owner="attention-test",
        lease_seconds=30,
        max_attempts=3,
        retry_min_seconds=0.01,
        retry_max_seconds=1,
        topics=topics,
    )


async def drain(factory, client, topics):
    worker = delivery(factory, client, topics)
    for _ in range(50):
        if not await worker.deliver_one():
            return
    raise AssertionError("delivery did not drain")


async def attention_rows(session):
    return list(
        await session.scalars(
            select(TransactionalOutbox)
            .where(
                TransactionalOutbox.payload["role"].astext == "attention",
            )
            .order_by(TransactionalOutbox.created_at)
        )
    )


async def test_question_dedup_answer_resolution_and_restart(postgres_dsn, topics):
    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        asked = await command(factory, goal_id, "ask_owner", {"question": "Нужен ли вход?"})
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        feed = [i for i, (_, thread, _) in enumerate(client.sent) if thread == 7]
        assert len(feed) == 1
        assert client.sent[feed[0]][2] == "🟡 Сайт школы\nНужен ответ на вопрос руководителя."
        assert client.sent_silent[feed[0]] is False
        assert all(
            silent
            for (_, thread, _), silent in zip(client.sent, client.sent_silent, strict=True)
            if thread == THREAD
        )
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.version += 1
            await enqueue_goal_status(session, goal)
            await enqueue_goal_status(session, goal)
            question = await session.scalar(
                select(TelegramMessageLink).where(
                    TelegramMessageLink.message_role == f"lq:{goal_id.hex}:{asked.version}",
                )
            )
            assert client.sent_keyboards[feed[0]] == (
                (("Открыть", f"https://t.me/c/1234567890/{THREAD}/{question.message_id}"),),
            )
            version = goal.version
        await drain(factory, client, topics)
        assert len([m for m in client.sent if m[1] == 7]) == 1
        result = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="answer_permission",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="answer-attention",
                payload={"question_version": asked.version, "answer": "Да"},
            ),
        )
        assert result.status == "applied"
        async with factory() as session:
            resolved = next(row for row in await attention_rows(session) if row.payload["resolved"])
            prepared = await prepare_delivery(session, resolved, topics)
            assert prepared.action is DeliveryAction.EDIT_STATUS
            assert prepared.disable_notification is True and prepared.callback_buttons == ()
        await drain(factory, client, topics)
        assert any(
            body == "✅ Сайт школы\nрешено: Нужен ответ на вопрос руководителя."
            for _, _, body in client.edited
        )
        # Replay all attention rows after a fresh delivery service has been constructed.
        before = len(client.sent)
        async with factory.begin() as session:
            for row in await attention_rows(session):
                row.status = DeliveryStatus.PENDING
        await drain(factory, client, topics)
        assert len(client.sent) == before
        async with factory() as session:
            assert (
                len([r for r in await attention_rows(session) if r.payload["revision"] == 1]) == 1
            )


async def test_late_project_message_replaces_thread_link_without_notification(postgres_dsn, topics):
    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        role = f"lq:{goal_id.hex}:99"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            await sync_attention(
                session, goal, "owner", decision="99", target={"message_role": role}
            )
        await drain(factory, client, topics)
        assert client.sent_keyboards[-1] == ((("Открыть", f"https://t.me/c/1234567890/{THREAD}"),),)
        async with factory.begin() as session:
            await enqueue_chat_notice(
                session,
                chat_id=CHAT,
                thread_id=THREAD,
                key="late-question",
                text="Нужен ответ?",
                projection_role=role,
                revision=99,
            )
        await drain(factory, client, topics)
        assert len([m for m in client.sent if m[1] == 7]) == 1
        assert client.sent_silent[-1] is True
        assert client.edited_keyboards[-1][0][0][1].count("/") == 6
        assert client.edited_keyboards[-1][0][0][1] != client.sent_keyboards[-2][0][0][1]


@pytest.mark.parametrize("action", ["approve", "discard", "replace"])
async def test_plan_decision_resolves_or_supersedes_attention(postgres_dsn, topics, action):
    from tests.integration.leadership.planning_helpers import seed_plan

    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        plan = plan_items(1)
        plan["items"][0]["suggested_risk"] = "high"
        result = await seed_plan(factory, goal_id, {"plan": plan})
        assert result.status == "applied"
        async with factory.begin() as session:
            await enqueue_goal_status(session, await session.get(ManagedGoal, goal_id))
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        assert len([m for m in client.sent if m[1] == 7]) == 1
        if action == "replace":
            plan["title"] = "Новый план"
            plan["items"][0]["summary"] = "Другое действие"
            replaced = await seed_plan(factory, goal_id, {"plan": plan})
            assert replaced.status == "applied"
        else:
            async with UnitOfWork(factory) as uow:
                goal = await uow.session.get(ManagedGoal, goal_id)
                package = await uow.session.get(WorkPackage, goal.current_package_id)
                revision = await uow.session.get(PlanRevision, package.head_revision_id)
                kwargs = dict(
                    package_id=package.id,
                    revision_number=revision.revision_number,
                    h8=revision.content_hash[:8],
                    expected_status_generation=package.version,
                    user_id=OWNER,
                )
                if action == "approve":
                    await WorkPackageService(uow).approve(
                        **kwargs,
                        content_hash=revision.content_hash,
                        scope_revision=goal.scope_revision,
                    )
                else:
                    await WorkPackageService(uow).discard(**kwargs)
        await drain(factory, client, topics)
        assert any("решено: Нужно одобрить план работы." in body for _, _, body in client.edited)
        assert len([m for m in client.sent if m[1] == 7]) == (2 if action == "replace" else 1)
        if action == "discard":
            # The owner's "no" wakes the lead instead of leaving the goal waiting.
            async with factory() as session:
                goal = await session.get(ManagedGoal, goal_id)
                assert goal.lifecycle == "active" and goal.waiting_reason is None
                assert await session.scalar(
                    select(TransactionalOutbox.id).where(
                        TransactionalOutbox.destination == "project_lead",
                        TransactionalOutbox.payload["wakeup_kind"].astext == "plan_discarded",
                    )
                )


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.WAITING_APPROVAL,
        TaskStatus.FAILED,
        TaskStatus.BLOCKED,
        TaskStatus.QUOTA_EXHAUSTED,
    ],
)
async def test_managed_task_events_resolve_on_resume_without_legacy_duplicates(
    postgres_dsn, topics, status
):
    from tests.integration.leadership.planning_helpers import seed_plan

    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        await seed_plan(factory, goal_id, {"plan": plan_items(1)})
        async with UnitOfWork(factory) as uow:
            goal = await uow.session.get(ManagedGoal, goal_id)
            package = await uow.session.get(WorkPackage, goal.current_package_id)
            record = await uow.tasks.create(
                user_id=OWNER,
                chat_id=CHAT,
                thread_id=THREAD,
                original_text="Работа",
                task_type="coding",
            )
            task = await uow.session.get(Task, record.id)
            task.status = status
            await publish_task_event(uow.session, task)
            assert await attention_rows(uow.session) == [], "legacy task must not create attention"
            plan_item = await uow.session.scalar(
                select(PlanRevisionItem).where(
                    PlanRevisionItem.plan_revision_id == package.head_revision_id,
                    PlanRevisionItem.ordinal == 1,
                )
            )
            uow.session.add(
                MaterializationLink(
                    work_package_id=package.id,
                    plan_revision_id=package.head_revision_id,
                    plan_revision_item_id=plan_item.id,
                    work_item_draft_id=plan_item.item_id,
                    ordinal=1,
                    task_id=task.id,
                )
            )
            await uow.session.flush()
            await publish_task_event(uow.session, task)
            task.version += 1
            await publish_task_event(uow.session, task)
            assert len(await attention_rows(uow.session)) == 1
            task_id = task.id
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        feed = [m for m in client.sent if m[1] == 7]
        assert len(feed) == 1
        assert feed[0][2].startswith("🟡" if status is TaskStatus.WAITING_APPROVAL else "🔴")
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            task.status = TaskStatus.EXECUTING
            task.version += 1
            await publish_task_event(session, task)
        await drain(factory, client, topics)
        assert any("решено:" in body for _, _, body in client.edited)
        assert len([m for m in client.sent if m[1] == 7]) == 1


async def test_planner_failure_and_confirmed_completion(postgres_dsn, topics):
    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            await publish_outcome(
                session, goal, uuid.uuid4(), reason="planner_provider_unavailable"
            )
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        assert any(thread == 7 and body.startswith("🔴") for _, thread, body in client.sent)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle = "completed"
            goal.version += 1
            await sync_goal_attention(session, goal, completion_confirmed=False)
        await drain(factory, client, topics)
        assert not any(thread == 7 and body.startswith("✅") for _, thread, body in client.sent)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            await sync_goal_attention(session, goal, completion_confirmed=True)
        await drain(factory, client, topics)
        index = next(
            i
            for i, (_, thread, body) in enumerate(client.sent)
            if thread == 7 and body.startswith("✅")
        )
        assert client.sent_silent[index] is True


async def test_attention_rolls_back_with_state(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle, goal.waiting_reason = "waiting", "owner"
            await enqueue_goal_status(session, goal)
            await session.rollback()
        async with factory() as session:
            assert await attention_rows(session) == []
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"


@pytest.mark.parametrize("reason", ["delegation_expired", "delegation_revoked"])
async def test_delegation_attention_is_reconciled_and_cancel_resolves(postgres_dsn, topics, reason):
    from datetime import UTC, datetime, timedelta

    from tests.integration.leadership.test_lead_review_regressions import control
    from tests.integration.leadership.test_managed_goal_flow import _worker
    from vuzol.leadership.model import UnconfiguredLeadModel
    from vuzol.leadership.persistence import GoalDelegation

    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        if reason == "delegation_revoked":
            assert (await control(factory, goal_id, "revoke_delegation")).status == "applied"
        else:
            async with factory.begin() as session:
                goal = await session.get(ManagedGoal, goal_id)
                grant = await session.get(GoalDelegation, goal.delegation_id)
                grant.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        worker = _worker(factory, UnconfiguredLeadModel())
        for _ in range(2):
            async with factory.begin() as session:
                await worker._reconcile(session)
        async with factory() as session:
            items = await attention_rows(session)
            assert len(items) == 1 and items[0].payload["reason"] == reason
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        assert len([m for m in client.sent if m[1] == 7 and m[2].startswith("🔴")]) == 1
        assert (await control(factory, goal_id, "cancel_goal")).status == "applied"
        await drain(factory, client, topics)
        assert any("решено:" in body for _, _, body in client.edited)


async def test_resolution_before_delivery_is_silent_and_concurrent_producers_dedup(
    postgres_dsn, topics
):
    import asyncio

    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)

        async def produce():
            async with factory.begin() as session:
                goal = await session.get(ManagedGoal, goal_id)
                await sync_attention(session, goal, "owner")

        await asyncio.gather(produce(), produce())
        async with factory.begin() as session:
            assert len(await attention_rows(session)) == 1
            goal = await session.get(ManagedGoal, goal_id)
            await sync_attention(session, goal, None)
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        feed = [i for i, (_, thread, _) in enumerate(client.sent) if thread == 7]
        assert len(feed) == 1
        assert client.sent_silent[feed[0]] is True
        assert "решено:" in client.sent[feed[0]][2]
        assert client.sent_keyboards[feed[0]] == ()


async def test_pending_owner_plan_survives_pause_and_a_lead_wait_for(postgres_dsn, topics):
    # Live pilot: after pause/resume the lead answered wait_for on the package. The
    # attention item closed although the owner never approved the plan.
    from tests.integration.leadership.planning_helpers import seed_plan

    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        plan = plan_items(1)
        plan["items"][0]["suggested_risk"] = "high"
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        async with factory.begin() as session:
            await enqueue_goal_status(session, await session.get(ManagedGoal, goal_id))
        client = FakeTelegramClient()
        await drain(factory, client, topics)
        assert len([m for m in client.sent if m[1] == 7]) == 1
        # The real pause path also pauses the draft package.
        assert (await command(factory, goal_id, "pause_goal", {})).status == "applied"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            package = await session.get(WorkPackage, goal.current_package_id)
            assert package.status.value == "paused"
            version, package_id = goal.version, goal.current_package_id
        resumed = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="resume_goal",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="resume-attention",
                payload={},
            ),
        )
        assert resumed.status == "applied"
        await drain(factory, client, topics)
        assert not any("решено" in body for _, _, body in client.edited)
        assert len([m for m in client.sent if m[1] == 7]) == 1
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle, goal.waiting_reason, goal.wakeup_condition = "active", None, None
        result = await command(
            factory,
            goal_id,
            "wait_for",
            {"condition": {"kind": "package", "package_id": str(package_id)}},
        )
        assert result.status == "waiting"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert (goal.lifecycle, goal.waiting_reason) == ("waiting", "plan_approval")
            await sync_goal_attention(session, goal)
        await drain(factory, client, topics)
        assert not any("решено" in body for _, _, body in client.edited)
        assert len([m for m in client.sent if m[1] == 7]) == 1


async def test_idle_lead_with_a_plan_on_the_owner_card_waits_for_approval(postgres_dsn, topics):
    # Live pilot: after plan_ready the lead returned no commands and the goal showed
    # "model_idle" with a recovery prompt, although it was waiting for the owner.
    from tests.integration.leadership.planning_helpers import seed_plan
    from tests.integration.leadership.test_managed_goal_flow import _worker
    from vuzol.leadership.model import LeadDecision

    async with _factory(postgres_dsn) as factory:
        goal_id = await seed(factory)
        plan = plan_items(1)
        plan["items"][0]["suggested_risk"] = "high"
        assert (await seed_plan(factory, goal_id, {"plan": plan})).status == "applied"
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle, goal.waiting_reason, goal.wakeup_condition = "active", None, None
            await enqueue_goal_status(session, goal)

        class Idle:
            async def decide(self, snapshot):
                return LeadDecision(reply="Жду одобрения плана")

        from vuzol.leadership.wakeups import enqueue_goal_wakeup

        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            await enqueue_goal_wakeup(
                session,
                goal_id=goal_id,
                package_id=goal.current_package_id,
                idempotency_key="idle-test",
                kind="resume",
            )
        worker = _worker(factory, Idle())
        for _ in range(3):
            await worker.process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert (goal.lifecycle, goal.waiting_reason) == ("waiting", "plan_approval")
