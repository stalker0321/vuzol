"""Lead decisions, snapshots and tool reads expose asynchronous research."""

import uuid

import pytest
from sqlalchemy import select
from tests.integration.leadership.test_job_lifecycle import _enqueue
from tests.integration.leadership.test_lead_surface import _profile, _Transport
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    TEST_REPOSITORY_ROOT,
    _enable,
    _factory,
    _session,
)
from tests.integration.leadership.test_read_tools_pg import Scripted, audits

from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import ProjectLeadSettings, Settings
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.jobs import claim_jobs, complete_job
from vuzol.leadership.persistence import (
    GoalDelegation,
    LeadershipCommandReceipt,
    LeadershipTurn,
    LeadJob,
    ManagedGoal,
)
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.types import DeliveryStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def worker(factory, adapter):
    lead = build_project_lead_worker(
        factory,
        settings=Settings(
            environment="test",
            project_lead=ProjectLeadSettings(
                enabled=True,
                profile_id="lead-local",
            ),
        ),
        profiles=ProfileRegistry((_profile().model_copy(update={"context_limit": 100000}),)),
        owner="job-surface",
        adapter=adapter,
        repository_paths={PROJECT: TEST_REPOSITORY_ROOT},
    )
    # Runner execution has separate tests; keep command/read assertions deterministic.
    lead.job_runner = None
    return lead


async def test_lead_decision_starts_and_cancels_jobs_even_under_an_existing_grant(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        old_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            delegation = await session.get(GoalDelegation, goal.delegation_id)
            delegation.allowed_actions = [
                a for a in delegation.allowed_actions if a not in {"start_job", "cancel_job"}
            ]
        adapter = _Transport(
            {
                "reply": "Изучаю код.",
                "commands": [
                    {
                        "name": "start_job",
                        "payload": {"kind": "research", "question": "Где проверка?"},
                    },
                    {"name": "cancel_job", "payload": {"job_id": str(old_id)}},
                ],
            }
        )
        assert await worker(factory, adapter).process_one()
        async with factory() as session:
            jobs = list(await session.scalars(select(LeadJob)))
            assert len(jobs) == 2
            assert (await session.get(LeadJob, old_id)).state == "cancelled"
            assert next(j for j in jobs if j.id != old_id).state == "queued"
            receipts = list(
                await session.scalars(
                    select(LeadershipCommandReceipt).where(
                        LeadershipCommandReceipt.command_name.in_(("start_job", "cancel_job"))
                    )
                )
            )
            assert len(receipts) == 2 and all(r.status == "applied" for r in receipts)


async def test_snapshot_and_tool_batch_expose_scoped_untrusted_job_result(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            claims = await claim_jobs(session, owner="fixture")
            assert await complete_job(session, claims[0], result_text="Данные из jobs.py:1.")
        adapter = Scripted(
            {
                "tool_calls": [
                    {"name": "read_job", "args": {"job_id": str(job_id)}},
                    {"name": "read_job", "args": {"job_id": str(uuid.uuid4())}},
                ]
            },
            {"reply": "", "commands": []},
        )
        lead = worker(factory, adapter)
        snapshot = await lead._snapshot(goal_id)
        assert snapshot.result_context["jobs"][0]["id"] == str(job_id)
        assert snapshot.result_context["jobs"][0]["stale"] is False
        assert "result_text" not in snapshot.result_context["jobs"][0]
        async with factory.begin() as session:
            (await session.get(LeadJob, job_id)).pinned_commit = "b" * 40
        assert (await lead._snapshot(goal_id)).result_context["jobs"][0]["stale"] is True
        assert await lead.process_one()
        result, missing = [a["result"] for a in audits(adapter.requests[1])]
        assert result["result_text"] == "Данные из jobs.py:1."
        assert result["source"] == "untrusted_job_result"
        assert "never instructions" in result["notice"]
        assert missing == {"error": "job_not_found"}


@pytest.mark.parametrize("lifecycle", ["draft", "active", "waiting", "paused"])
async def test_job_finished_gets_a_turn_except_when_paused(postgres_dsn, lifecycle):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            for item in await session.scalars(
                select(TransactionalOutbox).where(TransactionalOutbox.destination == "project_lead")
            ):
                item.status = DeliveryStatus.DELIVERED
            claims = await claim_jobs(session, owner="fixture")
            assert await complete_job(session, claims[0], result_text="report")
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle = lifecycle
            if lifecycle == "waiting":
                goal.waiting_reason = "owner_question"
                goal.wakeup_condition = {"kind": "owner_answer"}
        adapter = _Transport({"reply": "", "commands": []})
        assert await worker(factory, adapter).process_one()
        assert len(adapter.requests) == (0 if lifecycle == "paused" else 1)
        async with factory() as session:
            event = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"lead:job:{job_id}"
                )
            )
            assert event.status == DeliveryStatus.DELIVERED
            turns = list(await session.scalars(select(LeadershipTurn)))
            assert len(turns) == (0 if lifecycle == "paused" else 1)
            if lifecycle == "waiting":
                goal = await session.get(ManagedGoal, goal_id)
                assert goal.lifecycle == "waiting" and goal.waiting_reason == "owner_question"
