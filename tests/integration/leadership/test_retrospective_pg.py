# ruff: noqa: RUF001
"""Retrospective outbox, accounted inference and atomic terminal-goal memory."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from tests.integration.leadership import test_managed_goal_flow as flow
from tests.integration.leadership.test_lead_review_regressions import StartModel
from tests.integration.leadership.test_lead_surface import _profile, _settings, _Transport
from tests.integration.leadership.test_notebooks_pg import other_goal, remember
from tests.integration.leadership.test_task_control import command
from tests.integration.storage.helpers import seed_task_run_step

from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import assignment_revision
from vuzol.leadership.composition import build_goal_retrospective_worker
from vuzol.leadership.invocation_context import current_turn, invocation_current
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.persistence import GoalDelegation, LeadershipInference, ManagedGoal
from vuzol.leadership.retrospective import (
    DESTINATION,
    GoalRetrospectiveConsumer,
    enqueue_retrospective,
)
from vuzol.leadership.retrospective_input import MAX_INPUT_BYTES, retrospective_input
from vuzol.leadership.retrospective_model import RetrospectiveResponse
from vuzol.providers.domain import ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import (
    Event,
    MaterializationLink,
    MemoryUnit,
    ProjectExecutorPreference,
    ReviewOutcomeHistory,
    Run,
    Step,
    TransactionalOutbox,
    UsageRecord,
    WorkPackage,
)
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import DeliveryStatus, MemoryScope, MemoryUnitStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def response(notebook=()):
    return RetrospectiveResponse.model_validate(
        {
            "summary": "Проверили решение <цели>.",
            "decisions": [
                {
                    "title": "Атомарная запись",
                    "context": "При повторах возможны дубли",
                    "options": ["Транзакция", "Отдельные записи"],
                    "choice": "Транзакция",
                    "reason": "Нужна согласованность",
                    "links": {"task_ids": [], "commits": []},
                }
            ],
            "notebook": list(notebook),
        }
    )


class Model:
    def __init__(self, result=None):
        self.result = result if result is not None else response()
        self.calls = []

    async def retrospect(self, project_id, context, *, invocation_id):
        self.calls.append((project_id, context, invocation_id))
        return self.result


async def terminal(factory, goal_id, outcome="accepted"):
    async with factory.begin() as session:
        goal = await session.get(ManagedGoal, goal_id)
        goal.lifecycle = "completed" if outcome == "accepted" else "cancelled"
        if outcome == "accepted":
            goal.constraints = {
                **goal.constraints,
                "owner_acceptance": {"commit": "a" * 40, "branch": "main"},
            }
        await enqueue_retrospective(session, goal, outcome=outcome, reason="Приоритет изменился")


async def jobs(factory):
    async with factory() as session:
        return list(
            await session.scalars(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == DESTINATION)
            )
        )


async def memory_notices(session):
    return list(
        await session.scalars(
            select(TransactionalOutbox).where(
                TransactionalOutbox.idempotency_key.startswith("memory:")
                | TransactionalOutbox.idempotency_key.startswith("lead:retrospective-summary:")
            )
        )
    )


@pytest.mark.parametrize("worked", [False, True])
async def test_cancellation_queues_only_started_work_and_replays_once(postgres_dsn, worked):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        if worked:
            await flow._worker(factory, StartModel()).process_one()
            await flow._worker(factory, StartModel()).process_one()
        result = await command(
            factory, goal_id, "cancel_goal", {}, key="cancel", reason="Больше не нужно"
        )
        assert result.status == "applied"
        await command(factory, goal_id, "cancel_goal", {}, key="cancel", reason="Больше не нужно")
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            await enqueue_retrospective(session, goal, outcome="cancelled")
        queued = await jobs(factory)
        assert len(queued) == int(worked)
        if worked:
            assert queued[0].payload["reason"] == "Больше не нужно"
            assert await GoalRetrospectiveConsumer(
                factory, owner="retro", model=Model()
            ).process_one()
            assert (await jobs(factory))[0].status == DeliveryStatus.DELIVERED


async def test_enqueuing_rolls_back_with_terminal_transition(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        with pytest.raises(RuntimeError):
            async with factory.begin() as session:
                goal = await session.get(ManagedGoal, goal_id)
                goal.lifecycle = "completed"
                await enqueue_retrospective(session, goal, outcome="accepted")
                raise RuntimeError("rollback")
        assert not await jobs(factory)
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"


@pytest.mark.parametrize("ran", [False, True])
async def test_draft_package_does_not_count_but_started_materialized_run_does(postgres_dsn, ran):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        if ran:
            await flow._worker(factory, StartModel()).process_one()
            await flow._worker(factory, StartModel()).process_one()
            _, run_id, _ = await seed_task_run_step(factory)
            async with factory.begin() as session:
                link = await session.scalar(select(MaterializationLink))
                (await session.get(WorkPackage, link.work_package_id)).start_generation = None
                run = await session.get(Run, run_id)
                run.task_id, run.started_at = link.task_id, datetime.now(UTC)
        else:
            async with factory.begin() as session:
                goal = await session.get(ManagedGoal, goal_id)
                package = WorkPackage(
                    project_id=goal.project_id,
                    managed_goal_id=goal.id,
                    session_id=goal.discussion_session_id,
                    title="Черновик",
                )
                session.add(package)
                await session.flush()
                goal.current_package_id = package.id
        assert (await command(factory, goal_id, "cancel_goal", {})).status == "applied"
        assert len(await jobs(factory)) == int(ran)


async def test_empty_response_receipt_survives_replay(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await terminal(factory, goal_id)
        model = Model(RetrospectiveResponse(decisions=[], notebook=[], summary="Уроков нет."))
        consumer = GoalRetrospectiveConsumer(factory, owner="retro", model=model)
        assert await consumer.process_one()
        job = (await jobs(factory))[0]
        async with factory.begin() as session:
            (await session.get(TransactionalOutbox, job.id)).status = DeliveryStatus.PENDING
        assert await consumer.process_one()
        assert len(model.calls) == 1
        async with factory.begin() as session:
            assert len(await memory_notices(session)) == 1


async def test_crash_attempts_are_bounded_before_model_call(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await terminal(factory, goal_id)
        model = Model()
        consumer = GoalRetrospectiveConsumer(factory, owner="retro", model=model)
        job = (await jobs(factory))[0]
        async with factory.begin() as session:
            row = await session.get(TransactionalOutbox, job.id)
            row.attempt_count, row.status = 5, DeliveryStatus.PENDING
        assert await consumer.process_one()
        assert not model.calls
        assert (await jobs(factory))[0].status == DeliveryStatus.DEAD_LETTER
        async with factory() as session:
            assert not await memory_notices(session)


async def test_input_joins_historical_rejections_plans_tasks_and_notebooks(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await remember(factory, goal_id, "Правило проекта")
        await remember(factory, goal_id, "Предпочтение владельца", scope="global")
        await flow._worker(factory, StartModel()).process_one()
        await flow._worker(factory, StartModel()).process_one()
        _, run_id, step = await seed_task_run_step(factory)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            link = await session.scalar(select(MaterializationLink))
            (await session.get(Run, run_id)).task_id = link.task_id
            (await session.get(Step, step.id)).result = {
                "structured_output": {
                    "result_commit": "b" * 40,
                    "summary": "Исправили повторную доставку",
                }
            }
            goal.constraints = {
                **goal.constraints,
                "last_amendment": {"reason": "Нужна идемпотентность"},
            }
            session.add(
                Event(
                    entity_type="managed_goal",
                    entity_id=goal_id,
                    event_type="managed_goal.amended",
                    actor_type="user",
                    actor_id="42",
                    payload={"reason": "Сохранить историю"},
                )
            )
            for index in range(30):
                session.add(
                    ReviewOutcomeHistory(
                        task_id=link.task_id,
                        run_id=run_id,
                        step_id=step.id,
                        acceptance_key=f"{index:064x}",
                        verdict="BLOCKED",
                        summary=f"Отклонение {index}",
                        result_commit="b" * 40,
                        findings=[
                            {"title": "Повторы", "description": "я" * 10000, "path": "src/test.py"}
                        ],
                    )
                )
                session.add(
                    ReviewOutcomeHistory(
                        task_id=link.task_id,
                        run_id=run_id,
                        step_id=step.id,
                        acceptance_key=f"{100 + index:064x}",
                        verdict="pass",
                        summary="Поздняя приёмка",
                        created_at=datetime.now(UTC) + timedelta(seconds=10),
                    )
                )
            await session.flush()
            context = await retrospective_input(
                session, goal, outcome="cancelled", reason="Приоритет"
            )
            encoded = json.dumps(context, ensure_ascii=False).encode()
            assert len(encoded) <= MAX_INPUT_BYTES
            assert context["goal"]["request"] == goal.original_request
            assert context["outcome"]["reason"] == "Приоритет"
            assert context["plans"][0]["plan"]["items"]
            assert context["tasks"][0]["id"] == str(link.task_id)
            assert context["packages"][0]["id"] == str(link.work_package_id)
            assert context["constraints"]["last_amendment"]["reason"] == "Нужна идемпотентность"
            assert any(e["payload"].get("reason") == "Сохранить историю" for e in context["events"])
            assert context["reviews"][0]["verdict"] == "BLOCKED"
            assert context["reviews"][0]["summary"].startswith("Отклонение")
            assert context["reviews"][0]["findings"][0]["description"].endswith("[truncated]")
            assert context["reviews"][-1] == {"truncated": True}
            assert context["project_notebook"]["notes"][0]["text"] == "Правило проекта"
            assert context["global_notebook"]["notes"][0]["text"] == "Предпочтение владельца"


async def test_applies_decisions_notes_notices_once_without_touching_terminal_goal(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        old = await remember(factory, goal_id, "Старое правило")
        forgotten = await remember(factory, goal_id, "Устарело")
        other_id = await other_goal(factory)
        async with factory.begin() as session:
            units = MemoryUnitRepository(session)
            foreign = await units.create_note(
                scope=MemoryScope.PROJECT,
                project_id="other",
                goal_id=other_id,
                text="Чужой факт",
                extraction_identity="foreign",
            )
            foreign_id = foreign.id
        await terminal(factory, goal_id)
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            before = goal.version, goal.fence_epoch
            notice_count = len(await memory_notices(session))
        model = Model(
            response(
                [
                    {"action": "remember", "scope": "project", "text": "Новый факт"},
                    {
                        "action": "remember",
                        "scope": "project",
                        "text": "Новое правило",
                        "replaces": str(old),
                    },
                    {"action": "forget", "note_id": str(forgotten)},
                    {"action": "forget", "note_id": str(foreign_id)},
                ]
            )
        )
        consumer = GoalRetrospectiveConsumer(factory, owner="retro", model=model)
        assert await consumer.process_one()
        assert not await consumer.process_one()
        job = (await jobs(factory))[0]
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert (goal.version, goal.fence_epoch) == before
            units = MemoryUnitRepository(session)
            decisions = await units.decisions_for_goal(project_id=flow.PROJECT, goal_id=goal_id)
            assert len(decisions) == 1
            assert decisions[0].details == model.result.decisions[0].model_dump()
            assert decisions[0].text == model.result.decisions[0].render()
            assert not await units.decisions_for_goal(project_id="other", goal_id=goal_id)
            assert (await session.get(MemoryUnit, old)).status == MemoryUnitStatus.SUPERSEDED
            assert (await session.get(MemoryUnit, forgotten)).status == MemoryUnitStatus.RETRACTED
            assert (
                await session.get(MemoryUnit, foreign_id)
            ).status == MemoryUnitStatus.OBSERVATION
            notices = await memory_notices(session)
            assert len(notices) == notice_count + 4
            summary = next(
                n for n in notices if n.idempotency_key.startswith("lead:retrospective-summary:")
            )
            assert "📒 Итоги цели: Проверили решение &lt;цели&gt;." in summary.payload["html"]
            assert "+1, ~1 заменено, −1 забыто, пропущено 1" in summary.payload["html"]
            assert sum(bool(n.payload.get("callback_buttons")) for n in notices) == 4
            # Operational replay retains the atomic application receipt.
            row = await session.get(TransactionalOutbox, job.id)
            row.status = DeliveryStatus.PENDING
        assert await consumer.process_one()
        assert len(model.calls) == 1
        async with factory() as session:
            assert len(await memory_notices(session)) == notice_count + 4


async def test_budget_and_secret_refusals_are_skipped(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        for _ in range(3):
            await remember(factory, goal_id, "я" * 600, scope="global")
        await remember(factory, goal_id, "я" * 200, scope="global")
        await terminal(factory, goal_id)
        model = Model(
            response(
                [
                    {"action": "remember", "scope": "global", "text": "Ещё один факт"},
                    {
                        "action": "remember",
                        "scope": "project",
                        "text": "password=" + "test-only-value",
                    },
                    {"action": "remember", "scope": "project", "text": "Полезный урок"},
                ]
            )
        )
        assert await GoalRetrospectiveConsumer(factory, owner="retro", model=model).process_one()
        async with factory() as session:
            notes = await MemoryUnitRepository(session).notebook(
                scope=MemoryScope.PROJECT, project_id=flow.PROJECT
            )
            assert [note.text for note in notes.notes] == ["Полезный урок"]
            notice = next(
                n
                for n in await memory_notices(session)
                if n.idempotency_key.startswith("lead:retrospective-summary:")
            )
            assert "+1, ~0 заменено, −0 забыто, пропущено 2" in notice.payload["html"]


@pytest.mark.parametrize("failure", ["provider", "invalid", "write"])
async def test_failure_retries_then_dead_letters_without_partial_writes(
    postgres_dsn, monkeypatch, caplog, failure
):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await terminal(factory, goal_id)
        model = Model(response([{"action": "remember", "scope": "project", "text": "Урок"}]))
        if failure == "provider":

            async def fail(*args, **kwargs):
                raise LeadModelUnavailable("provider_unavailable")

            model.retrospect = fail
        elif failure == "invalid":
            model.result = {"summary": "ok", "decisions": [], "notebook": [{"action": "bad"}]}
        else:

            async def fail(*args, **kwargs):
                raise RuntimeError("summary enqueue failed")

            monkeypatch.setattr("vuzol.leadership.retrospective.enqueue_chat_notice", fail)
        consumer = GoalRetrospectiveConsumer(factory, owner="retro", model=model, max_attempts=2)
        assert await consumer.process_one()
        job = (await jobs(factory))[0]
        assert job.status == DeliveryStatus.PENDING and job.attempt_count == 1
        assert job.available_at > job.created_at and job.last_error_category
        async with factory.begin() as session:
            assert not await session.scalar(select(MemoryUnit.id))
            assert not await memory_notices(session)
            (await session.get(TransactionalOutbox, job.id)).available_at = datetime.now(
                UTC
            ) - timedelta(seconds=1)
        assert await consumer.process_one()
        assert (await jobs(factory))[0].status == DeliveryStatus.DEAD_LETTER
        assert not await consumer.process_one()
        assert "dead-lettered" in caplog.text
        async with factory() as session:
            assert not await session.scalar(select(MemoryUnit.id))
            assert not await memory_notices(session)


async def test_composition_uses_project_lead_assignment_and_accounts_terminal_inference(
    postgres_dsn, monkeypatch
):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await terminal(factory, goal_id)
        profile = _profile().model_copy(update={"id": "project-lead", "context_limit": 32000})
        async with factory.begin() as session:
            session.add(
                ProjectExecutorPreference(
                    project_id=flow.PROJECT,
                    model_assignments={
                        "lead": {
                            "profile_id": profile.id,
                            "model": profile.model,
                            "reasoning_effort": profile.model_reasoning_effort,
                            "content_revision": assignment_revision(profile),
                        }
                    },
                )
            )
            goal = await session.get(ManagedGoal, goal_id)
            # Revocation must not block learning after the goal has finished.
            (await session.get(GoalDelegation, goal.delegation_id)).revoked_at = datetime.now(UTC)
        transport = _Transport(response().model_dump())
        consumer = build_goal_retrospective_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(), profile)),
            owner="retro",
            adapter=transport,
        )
        assert await consumer.process_one()
        assert (await jobs(factory))[0].status == DeliveryStatus.DELIVERED
        assert len(transport.requests) == 1
        request = transport.requests[0]
        assert request.output_schema_name == "lead_retrospective"
        async with factory() as session:
            usage = await session.scalar(
                select(UsageRecord).where(UsageRecord.invocation_id == request.task_id)
            )
            assert usage.purpose == "lead_retrospective" and usage.profile_id == profile.id
            assert usage.task_id is None and usage.cost_units > 0
            inference = await session.get(LeadershipInference, request.task_id)
            assert inference.state == "received"


async def test_disabled_consumer_and_lost_lease_cannot_apply(postgres_dsn):
    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await terminal(factory, goal_id)
        entered, release = asyncio.Event(), asyncio.Event()

        class Waiting(Model):
            async def retrospect(self, project_id, context, *, invocation_id):
                authority = current_turn.get()
                async with factory() as session:
                    assert await invocation_current(session, authority)
                entered.set()
                await release.wait()
                async with factory() as session:
                    assert not await invocation_current(session, authority)
                return response()

        consumer = GoalRetrospectiveConsumer(
            factory, owner="retro", model=Waiting(), enabled=lambda: False
        )
        assert not await consumer.process_one()
        consumer._enabled = lambda: True
        task = asyncio.create_task(consumer.process_one())
        await asyncio.wait_for(entered.wait(), timeout=10)
        async with factory.begin() as session:
            item = await session.scalar(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == DESTINATION)
            )
            item.lease_generation += 1
        release.set()
        assert await task
        async with factory() as session:
            assert not await session.scalar(select(MemoryUnit.id))
            assert not await memory_notices(session)


async def test_provider_failure_is_accounted_and_a_later_attempt_succeeds(
    postgres_dsn, monkeypatch
):
    class Transport(_Transport):
        failed = False

        async def execute(self, request, profile, cancellation):
            if not self.failed:
                self.failed = True
                raise ProviderFailure(
                    ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                    retryable=True,
                    request_sent=False,
                    safe_summary="test provider unavailable",
                )
            return await super().execute(request, profile, cancellation)

    async with flow._factory(postgres_dsn) as factory:
        goal_id = await flow._enable(factory, await flow._session(factory))
        await terminal(factory, goal_id)
        transport = Transport(response().model_dump())
        consumer = build_goal_retrospective_worker(
            factory,
            settings=_settings(monkeypatch, "lead-local"),
            profiles=ProfileRegistry((_profile(),)),
            owner="retro",
            adapter=transport,
        )
        assert await consumer.process_one()
        job = (await jobs(factory))[0]
        assert job.status == DeliveryStatus.PENDING
        async with factory.begin() as session:
            inference = await session.scalar(select(LeadershipInference))
            assert inference.state == "not_sent"
            assert not await session.scalar(select(MemoryUnit.id))
            assert not await memory_notices(session)
            (await session.get(TransactionalOutbox, job.id)).available_at = datetime.now(
                UTC
            ) - timedelta(seconds=1)
        assert await consumer.process_one()
        assert (await jobs(factory))[0].status == DeliveryStatus.DELIVERED
        async with factory() as session:
            assert len(await memory_notices(session)) == 1
            assert (
                len(
                    await MemoryUnitRepository(session).decisions_for_goal(
                        project_id=flow.PROJECT, goal_id=goal_id
                    )
                )
                == 1
            )
