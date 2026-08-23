import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import storage
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.providers.subscription_limits import LimitWindow, SubscriptionLimitSnapshot
from vuzol.storage.models import (
    Approval,
    MaterializationLink,
    PlanRevisionItem,
    ProjectExecutorPreference,
    Step,
    Task,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.storage.types import (
    ApprovalStatus,
    IdempotencyClass,
    PlanRevisionCreatedBy,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.projections import (
    FakeTelegramClient,
    LostTelegramResponse,
    StatusCard,
    apply_status_projection,
    build_approval_card,
    build_project_status_dashboard,
    build_status_card,
    build_task_history_report,
    enqueue_project_status_dashboard,
    enqueue_task_history_report,
    enqueue_task_status_projection,
    enqueue_terminal_task_projections,
)
from vuzol.telegram.work_packages import WorkPackageCallbackKind, parse_work_package_callback
from vuzol.workflows.result_approval import envelope_hash

pytestmark = pytest.mark.postgresql


def test_status_card_rebuild_and_revision_guard(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text='<script>alert("x")</script>',
                task_type="coding",
            )
            assert uow.session is not None
            stored = await uow.session.get(Task, task.id)
            assert stored is not None
            stored.task_draft = {
                "normalized_title": "Improve task cards",
                "task_summary": "Show a concise task description in Telegram",
            }
        client = FakeTelegramClient()
        async with factory() as session, session.begin():
            card = await build_status_card(session, task.id)
            assert "<b>Задача №100001</b>" in card.html
            assert "Задача: Show a concise task description in Telegram" in card.html
            assert "&lt;script&gt;" not in card.html
            assert await apply_status_projection(
                session, client, card=card, chat_id=-100, thread_id=10
            )
        async with factory() as session, session.begin():
            stale = StatusCard(task.id, card.revision - 1, "stale")
            assert not await apply_status_projection(
                session, client, card=stale, chat_id=-100, thread_id=10
            )
            newer = StatusCard(task.id, card.revision + 1, "new")
            assert await apply_status_projection(
                session, client, card=newer, chat_id=-100, thread_id=10
            )
        assert len(client.sent) == 1
        assert client.edited == [(-100, 1, "new")]
        await engine.dispose()

    asyncio.run(scenario())


def test_failed_or_lost_send_does_not_create_projection_link(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                original_text="request",
                task_type="coding",
            )
            task_id = task.id
        client = FakeTelegramClient(fail=LostTelegramResponse("unknown outcome"))
        async with factory() as session:
            with pytest.raises(LostTelegramResponse):
                await apply_status_projection(
                    session,
                    client,
                    card=StatusCard(task_id, 1, "status"),
                    chat_id=-100,
                    thread_id=10,
                )
            await session.rollback()
        async with factory() as session:
            assert await session.scalar(select(TelegramMessageLink.id)) is None
        await engine.dispose()

    asyncio.run(scenario())


def test_completed_agent_result_is_rendered_in_project_topic(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Review the architecture",
                task_type="architecture",
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="architecture",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.COMPLETED,
            )
            step = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="execute_agent",
                status=StepStatus.COMPLETED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            assert uow.session is not None
            stored_task = await uow.session.get(Task, task.id)
            stored_step = await uow.session.get(Step, step.id)
            assert stored_task is not None and stored_step is not None
            stored_task.status = TaskStatus.COMPLETED
            stored_step.executor_profile_id = "codex-subscription-prod"
            stored_step.result = {
                "model": "gpt-5.6-sol",
                "text": ("Use <ports> and adapters.\n\nPlan:\n- Rewrite the service next."),
            }

        async with factory() as session:
            card = await build_status_card(session, task.id)
            assert "<b>Отчёт о выполнении</b>" in card.html  # noqa: RUF001
            assert "Use &lt;ports&gt; and adapters." in card.html
            assert "Исполнитель: Codex Sol" in card.html
            assert "Rewrite the service next" not in card.html
        await engine.dispose()

    asyncio.run(scenario())


def test_failed_result_reports_stage_and_reason_in_project_topic(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Change the API",
                task_type="coding",
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.FAILED,
            )
            step = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="validate",
                status=StepStatus.FAILED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            assert uow.session is not None
            stored_task = await uow.session.get(Task, task.id)
            stored_step = await uow.session.get(Step, step.id)
            assert stored_task is not None and stored_step is not None
            stored_task.status = TaskStatus.FAILED
            stored_step.failure_category = "validation_failed"
            stored_step.failure_summary = "API contract test failed."

        async with factory() as session:
            card = await build_status_card(session, task.id)
            assert "Завершена неудачно" in card.html
            assert "<b>Отчёт о завершении</b>" in card.html  # noqa: RUF001
            assert "<b>Этап:</b> Проверка" in card.html
            assert "API contract test failed." in card.html
        await engine.dispose()

    asyncio.run(scenario())


def test_blocked_task_is_not_active_or_in_project_dashboard(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            blocked = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Blocked task marker",
                task_type="coding",
                task_draft={"task_summary": "Blocked task marker"},
            )
            active = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Active task marker",
                task_type="coding",
                task_draft={"task_summary": "Active task marker"},
            )
            assert uow.session is not None
            blocked_task = await uow.session.get(Task, blocked.id)
            active_task = await uow.session.get(Task, active.id)
            assert blocked_task is not None and active_task is not None
            blocked_task.status = TaskStatus.BLOCKED
            active_task.status = TaskStatus.EXECUTING
            await uow.session.flush()

            active_records = await uow.tasks.active_in_topic(-100, 10)
            assert [record.id for record in active_records] == [active.id]
            dashboard = await build_project_status_dashboard(uow.session, -100)
            assert "Active task marker" in dashboard.html
            assert "Blocked task marker" not in dashboard.html
        await engine.dispose()

    asyncio.run(scenario())


def _two_item_plan() -> PlanDraft:
    return PlanDraft(
        title="Ship the feature",
        items=tuple(
            PlanItemDraft(
                local_id=f"item-{ordinal}",
                summary=f"Step {ordinal}",
                goal=f"Goal {ordinal}",
                expected_outcome=f"Outcome {ordinal}",
                completion_criteria=(f"Check {ordinal}",),
                allowed_scope="src/**",
            )
            for ordinal in range(1, 3)
        ),
    )


async def _materialize_plan_item(
    factory: async_sessionmaker[AsyncSession],
    *,
    ordinal: int,
) -> tuple[uuid.UUID, uuid.UUID]:
    async with UnitOfWork(factory) as uow:
        # One active discussion per (chat, thread): derive a distinct thread per item.
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-1001, message_thread_id=90 + ordinal
        )
        result = await WorkPackageService(uow).create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_two_item_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
        )
        task = await uow.tasks.create(
            user_id=42,
            chat_id=-100,
            thread_id=10,
            project_id="vuzol",
            original_text=f"plan item {ordinal}",
            task_type="coding",
        )
        item = await uow.session.scalar(
            select(PlanRevisionItem).where(
                PlanRevisionItem.plan_revision_id == result.revision_id,
                PlanRevisionItem.ordinal == ordinal,
            )
        )
        assert item is not None
        await uow.work_packages.add_materialization(
            MaterializationLink(
                work_package_id=result.package_id,
                plan_revision_id=result.revision_id,
                work_item_draft_id=item.item_id,
                plan_revision_item_id=item.id,
                task_id=task.id,
                ordinal=ordinal,
            )
        )
    return task.id, result.package_id


def test_status_card_requires_existing_task(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with factory() as session:
            with pytest.raises(LookupError, match="task not found"):
                await build_status_card(session, uuid.uuid4())
        await engine.dispose()

    asyncio.run(scenario())


def test_work_package_item_card_encodes_edit_and_retry_callbacks(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        active_id, active_package = await _materialize_plan_item(factory, ordinal=1)
        failed_id, _failed_package = await _materialize_plan_item(factory, ordinal=2)
        async with factory() as session, session.begin():
            failed = await session.get(Task, failed_id)
            assert failed is not None
            failed.status = TaskStatus.FAILED

        async with factory() as session:
            card = await build_status_card(session, active_id)
            stored = await session.get(Task, active_id)
            assert stored is not None
            assert "Пункт плана: <b>1/2</b>" in card.html
            assert card.work_package_id == active_package
            assert card.control_status_generation == 1
            assert card.revision == stored.version * 10_000 + 1
            (row,) = card.callback_buttons
            assert [label for label, _ in row] == ["Изменить"]
            parsed = parse_work_package_callback(row[0][1])
            assert parsed.kind is WorkPackageCallbackKind.CONTINUE_DISCUSSION
            assert parsed.package_id == active_package
            assert parsed.revision_number == 1

            failed_card = await build_status_card(session, failed_id)
        assert "Отчёт о завершении" in failed_card.html  # noqa: RUF001
        assert "Причина не указана" in failed_card.html
        (failed_row,) = failed_card.callback_buttons
        retry_labels = [label for label, _ in failed_row]
        assert retry_labels == ["Повторить", "Изменить"]
        retry_kinds = {parse_work_package_callback(data).kind for _, data in failed_row}
        assert retry_kinds == {
            WorkPackageCallbackKind.RETRY_ITEM,
            WorkPackageCallbackKind.CONTINUE_DISCUSSION,
        }
        await engine.dispose()

    asyncio.run(scenario())


def test_terminal_history_report_shows_worker_preview_and_token_totals(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            uow.session.add(  # type: ignore[union-attr]
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=40,
                    topic_kind="changelog",
                    default_workflow="coding_task",
                )
            )
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=10,
                project_id="vuzol",
                original_text="Publish docs",
                task_type="docs",
                task_draft={"task_summary": "Publish the docs site"},
            )
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="docs",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.COMPLETED,
            )
            agent_step = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="execute_agent",
                status=StepStatus.COMPLETED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            publish_step = await uow.steps.create(
                run_id=run_id,
                ordinal=2,
                step_type="publish_static",
                status=StepStatus.COMPLETED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            stored_agent = await uow.session.get(Step, agent_step.id)
            stored_publish = await uow.session.get(Step, publish_step.id)
            assert stored_agent is not None and stored_publish is not None
            stored_agent.executor_profile_id = "codex-subscription-prod"
            stored_agent.result = {"model": "gpt-5.6-sol", "text": "Docs published"}
            stored_publish.result = {
                "status": "published",
                "public_url": "https://preview.vuzol.local/docs",
            }
            uow.session.add(  # type: ignore[union-attr]
                UsageRecord(
                    provider="codex",
                    profile_id="codex-subscription-prod",
                    model="gpt-5.6-sol",
                    task_id=task.id,
                    run_id=run_id,
                    step_id=agent_step.id,
                    input_tokens=1200,
                    output_tokens=340,
                    cached_tokens=100,
                    cost_units=Decimal("0.5"),
                    duration_ms=125_000,
                    outcome="ok",
                )
            )
            stored_task = await uow.session.get(Task, task.id)
            assert stored_task is not None
            stored_task.status = TaskStatus.COMPLETED

        async with factory() as session:
            report = await build_task_history_report(session, task.id)
            assert report is not None
            assert (report.chat_id, report.thread_id) == (-100, 40)
            assert "<b>#100001</b>" in report.html
            assert "Завершена успешно" in report.html
            assert "<b>Результат:</b> Docs published" in report.html
            assert "<b>Исполнитель:</b> Codex Sol" in report.html
            assert 'href="https://preview.vuzol.local/docs"' in report.html
            assert "1,200" in report.html and "340" in report.html and "100" in report.html
            assert "Работа: <code>2 мин 5 с</code>" in report.html  # noqa: RUF001
        await engine.dispose()

    asyncio.run(scenario())


def test_history_report_guards_on_status_and_expected_status(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        completed_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            uow.session.add(  # type: ignore[union-attr]
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=40,
                    topic_kind="changelog",
                    default_workflow="coding_task",
                )
            )
            fresh = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=11,
                original_text="not done yet",
                task_type="coding",
            )
            done = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=12,
                original_text="done",
                task_type="coding",
            )
            completed_id = done.id
            stored_done = await uow.session.get(Task, done.id)
            assert stored_done is not None
            stored_done.status = TaskStatus.COMPLETED

        async with factory() as session:
            assert await build_task_history_report(session, fresh.id) is None
            mismatch = await build_task_history_report(
                session, completed_id, expected_status=TaskStatus.FAILED
            )
            assert mismatch is None
            matched = await build_task_history_report(
                session, completed_id, expected_status=TaskStatus.COMPLETED
            )
            assert matched is not None
        await engine.dispose()

    asyncio.run(scenario())


def test_enqueue_history_report_is_idempotent_per_outcome(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            uow.session.add(  # type: ignore[union-attr]
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=40,
                    topic_kind="changelog",
                    default_workflow="coding_task",
                )
            )
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=13,
                original_text="boom",
                task_type="coding",
            )
            task_id = task.id
            stored = await uow.session.get(Task, task.id)
            assert stored is not None
            stored.status = TaskStatus.FAILED

        for _ in range(2):
            async with factory() as session, session.begin():
                await enqueue_task_history_report(session, task_id)

        async with factory() as session:
            rows = (
                (
                    await session.execute(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "telegram"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].payload["role"] == "task_history"
        assert rows[0].payload["terminal_status"] == "failed"
        assert rows[0].payload["message_thread_id"] == 40
        await engine.dispose()

    asyncio.run(scenario())


def test_dashboard_refresh_without_mapping_is_a_noop(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with factory() as session, session.begin():
            await enqueue_project_status_dashboard(session, -777)
            count = await session.scalar(select(func.count()).select_from(TransactionalOutbox))
        assert count == 0
        await engine.dispose()

    asyncio.run(scenario())


def test_threadless_task_refresh_updates_only_global_dashboard(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            uow.session.add(  # type: ignore[union-attr]
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=30,
                    topic_kind="task_dashboard",
                    default_workflow="coding_task",
                )
            )
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                original_text="chat-level request",
                task_type="coding",
            )
            task_id = task.id

        async with factory() as session, session.begin():
            stored = await session.get(Task, task_id)
            assert stored is not None
            await enqueue_task_status_projection(session, stored)

        async with factory() as session:
            rows = (
                (
                    await session.execute(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "telegram"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 1
            assert rows[0].payload["role"] == "project_status_dashboard"
            assert rows[0].payload["message_thread_id"] == 30
        await engine.dispose()

    asyncio.run(scenario())


def test_waiting_approval_projection_renders_package_action_outboxes(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id, package_id = await _materialize_plan_item(factory, ordinal=1)
        async with factory() as session, session.begin():
            stored = await session.get(Task, task_id)
            assert stored is not None
            stored.status = TaskStatus.WAITING_APPROVAL
            await enqueue_task_status_projection(session, stored)

        async with factory() as session:
            rows = (
                (
                    await session.execute(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "work_package_projection"
                        )
                    )
                )
                .scalars()
                .all()
            )
        operations = sorted(row.operation_type for row in rows)
        assert operations == ["render_action", "render_status"]
        assert all(row.payload["package_id"] == str(package_id) for row in rows)
        telegram_rows = (
            (
                await session.execute(
                    select(TransactionalOutbox).where(TransactionalOutbox.destination == "telegram")
                )
            )
            .scalars()
            .all()
        )
        assert len(telegram_rows) == 1
        assert telegram_rows[0].payload["role"] == "intake_ack"
        await engine.dispose()

    asyncio.run(scenario())


def test_terminal_projections_sequence_work_package_observation_once(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        linked_id, package_id = await _materialize_plan_item(factory, ordinal=1)
        plain_id = uuid.uuid4()
        async with UnitOfWork(factory) as uow:
            uow.session.add(  # type: ignore[union-attr]
                TopicMapping(
                    chat_id=-100,
                    message_thread_id=40,
                    topic_kind="changelog",
                    default_workflow="coding_task",
                )
            )
            plain = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=14,
                original_text="no package here",
                task_type="coding",
            )
            plain_id = plain.id
        for task_id in (plain_id, linked_id):
            async with factory() as session, session.begin():
                stored = await session.get(Task, task_id)
                assert stored is not None
                stored.status = TaskStatus.FAILED
                await enqueue_terminal_task_projections(session, stored)

        async with factory() as session:
            observations = (
                (
                    await session.execute(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "work_package_sequence"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(observations) == 1
        payload = observations[0].payload
        assert payload["work_package_id"] == str(package_id)
        assert payload["ordinal"] == 1
        assert payload["task_status"] == "failed"
        await engine.dispose()

    asyncio.run(scenario())


def test_approval_cards_render_pending_question_then_final_decision(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        now = datetime.now(UTC)
        envelope = {
            "step_id": None,
            "changed_files": ["src/auth.py", "src/auth_test.py"],
            "gates": [{"name": "tests", "duration_ms": 4200}, "junk-entry"],
        }
        async with UnitOfWork(factory) as uow:
            task = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=15,
                project_id="vuzol",
                original_text="rewrite auth",
                task_type="coding",
            )
            task_id = task.id
            run_id = await uow.runs.create(
                task_id=task.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
            )
            step = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="approval",
                status=StepStatus.WAITING_APPROVAL,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            envelope["step_id"] = str(step.id)
            stored_step = await uow.session.get(Step, step.id)
            assert stored_step is not None
            stored_step.payload = {"action_envelope": envelope}
            uow.session.add(  # type: ignore[union-attr]
                Approval(
                    step_id=step.id,
                    action_envelope_hash=envelope_hash(envelope),
                    requested_action="apply_result",
                    normalized_target="apply accepted result",
                    human_summary="Rewrite auth module",
                    token_hash=(uuid.uuid4().hex + uuid.uuid4().hex),
                    status=ApprovalStatus.PENDING,
                    requested_at=now,
                    expires_at=now + timedelta(hours=1),
                )
            )

        async with factory() as session:
            pending = await build_approval_card(session, task_id)
            assert pending.buttons == ("approve", "redo", "reject")
            assert pending.approval_id is not None
            assert "Что изменится" in pending.html
            assert "<code>src/auth.py, src/auth_test.py</code>" in pending.html
            assert "✅ tests — пройдено (4.2 с)" in pending.html  # noqa: RUF001
            assert "junk-entry" not in pending.html
            assert "Применить этот результат локально?" in pending.html

        async with factory() as session, session.begin():
            approval_row = await session.scalar(select(Approval).limit(1))
            assert approval_row is not None
            approval_row.status = ApprovalStatus.REJECTED
            approval_row.decided_at = now + timedelta(minutes=5)

        async with factory() as session:
            decided = await build_approval_card(session, task_id)
            assert decided.buttons == ()
            assert "Решение: <b>Отклонено</b>" in decided.html
            assert "✅ tests — пройдено (4.2 с)" in decided.html  # noqa: RUF001
            assert "Применить этот результат локально?" not in decided.html
        await engine.dispose()

    asyncio.run(scenario())


def test_dashboard_resolves_executors_pins_and_subscription_limits(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with UnitOfWork(factory) as uow:
            uow.session.add(  # type: ignore[union-attr]
                ProjectExecutorPreference(
                    project_id="vuzol",
                    mode="pin",
                    worker_key="sol",
                    reasoning_effort="medium",
                    revision=1,
                )
            )
            pinned = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=20,
                project_id="vuzol",
                original_text="pinned executor",
                task_type="coding",
            )
            grok = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=21,
                project_id="vuzol",
                original_text="grok running",
                task_type="coding",
            )
            modeled = await uow.tasks.create(
                user_id=42,
                chat_id=-100,
                thread_id=22,
                project_id="vuzol",
                original_text="model from result",
                task_type="coding",
            )
            for holder in (pinned, grok, modeled):
                stored = await uow.session.get(Task, holder.id)
                assert stored is not None
                stored.status = TaskStatus.EXECUTING
            grok_run = await uow.runs.create(
                task_id=grok.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.RUNNING,
            )
            grok_step = await uow.steps.create(
                run_id=grok_run,
                ordinal=1,
                step_type="execute_model",
                status=StepStatus.RUNNING,
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
            )
            stored_grok_step = await uow.session.get(Step, grok_step.id)
            assert stored_grok_step is not None
            stored_grok_step.executor_profile_id = "grok-build-prod"
            model_run = await uow.runs.create(
                task_id=modeled.id,
                workflow_type="coding",
                workflow_version="1",
                budget_mode="balanced",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
                status=RunStatus.RUNNING,
            )
            junk_step = await uow.steps.create(
                run_id=model_run,
                ordinal=1,
                step_type="execute_model",
                status=StepStatus.COMPLETED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            good_step = await uow.steps.create(
                run_id=model_run,
                ordinal=2,
                step_type="execute_model",
                status=StepStatus.COMPLETED,
                idempotency_class=IdempotencyClass.READ_ONLY,
            )
            stored_junk = await uow.session.get(Step, junk_step.id)
            stored_good = await uow.session.get(Step, good_step.id)
            assert stored_junk is not None and stored_good is not None
            stored_junk.executor_profile_id = "api"
            stored_junk.result = "not-a-mapping"
            stored_good.executor_profile_id = "api"
            stored_good.result = {"model": "qwen3-max"}

        snapshot = SubscriptionLimitSnapshot(
            profile_id="codex-subscription-prod",
            company="OpenAI",
            plan_label="Pro",
            five_hour=LimitWindow(
                remaining_percent=80,
                reset_at=datetime.now(UTC) + timedelta(hours=3),
                window_seconds=18_000,
            ),
            weekly=LimitWindow(remaining_percent=55, reset_at=None),
            observed_at=datetime.now(UTC),
            ok=True,
        )
        async with factory() as session:
            dashboard = await build_project_status_dashboard(
                session,
                -100,
                project_names={"vuzol": "Vuzol"},
                subscription_snapshots=[snapshot],
            )
            again = await build_project_status_dashboard(
                session,
                -100,
                project_names={"vuzol": "Vuzol"},
                subscription_snapshots=[snapshot],
            )
        assert dashboard.revision == again.revision
        assert dashboard.html == again.html
        assert "Sol · medium (по умолчанию для проекта)" in dashboard.html
        assert "Grok Build" in dashboard.html
        assert "Qwen3 Max" in dashboard.html
        assert "Лимиты подписки" in dashboard.html
        assert "OpenAI Pro" in dashboard.html
        assert "Обновлено:" in dashboard.html
        await engine.dispose()

    asyncio.run(scenario())
