"""Job lifecycle against PostgreSQL, independent of lead turns and provider calls."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    TEST_REPOSITORY_ROOT,
    _enable,
    _factory,
    _lead_service,
    _session,
)

from vuzol.config.settings import HardLimits
from vuzol.discussion.domain import DomainError
from vuzol.execution.git import LocalGit
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.jobs import (
    cancel_goal_jobs,
    cancel_job,
    claim_jobs,
    complete_job,
    enqueue_job,
    list_jobs,
    read_job,
    recover_expired_jobs,
)
from vuzol.leadership.persistence import LeadJob, ManagedGoal
from vuzol.storage.models import TransactionalOutbox, WorkPackage
from vuzol.workflows.application import Principal

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _enqueue(factory, goal_id, **kwargs):
    async with factory.begin() as session:
        job = await enqueue_job(
            session,
            goal_id=goal_id,
            repository=TEST_REPOSITORY_ROOT,
            payload={"kind": "research", "question": "Where is admission checked?"},
            **kwargs,
        )
        return job.id


async def test_enqueue_pins_commit_and_enforces_concurrent_and_lifetime_limits(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        jobs = [await _enqueue(factory, goal_id) for _ in range(2)]
        async with factory() as session:
            job = await session.get(LeadJob, jobs[0])
            assert job.state == "queued" and job.scope_revision == 1
            assert job.pinned_commit == await LocalGit().read_ref(TEST_REPOSITORY_ROOT, "main")
            assert await session.scalar(select(func.count()).select_from(WorkPackage)) == 0
        with pytest.raises(DomainError, match="job_nonterminal_limit"):
            await _enqueue(factory, goal_id)
        for index in range(10):
            if index >= 2:
                jobs.append(await _enqueue(factory, goal_id))
            async with factory.begin() as session:
                await cancel_job(session, goal_id=goal_id, job_id=jobs[index])
        with pytest.raises(DomainError, match="job_total_limit"):
            await _enqueue(factory, goal_id)


@pytest.mark.parametrize("operation", ["enqueue", "cancel"])
@pytest.mark.parametrize("lifecycle", ["paused", "completed", "cancelled"])
async def test_job_control_denies_inactive_goals(postgres_dsn, lifecycle, operation):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle = lifecycle
        reason = "goal_paused" if lifecycle == "paused" else "goal_terminal"
        with pytest.raises(DomainError, match=reason):
            if operation == "enqueue":
                await _enqueue(factory, goal_id)
            else:
                async with factory.begin() as session:
                    await cancel_job(session, goal_id=goal_id, job_id=job_id)


@pytest.mark.parametrize("lifecycle", ["draft", "active", "waiting"])
async def test_enqueue_allows_research_before_planning_and_while_waiting(postgres_dsn, lifecycle):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        async with factory.begin() as session:
            goal = await session.get(ManagedGoal, goal_id)
            goal.lifecycle = lifecycle
        assert await _enqueue(factory, goal_id)


async def test_claim_capacity_is_shared_across_runners_and_completion_is_atomic(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        for _ in range(3):
            await _enqueue(factory, goal_id, limits=HardLimits(lead_jobs_nonterminal_per_goal=3))
        async with factory.begin() as session:
            claims = await claim_jobs(session, owner="runner-a", concurrency=2, lease_seconds=60)
            assert len(claims) == 2
        async with factory.begin() as session:
            assert await claim_jobs(session, owner="runner-b", concurrency=2) == []
        async with factory.begin() as session:
            assert await complete_job(
                session, claims[0], result_text="я" * 20000, usage={"cost_units": "0.25"}
            )
        async with factory.begin() as session:
            assert not await complete_job(session, claims[0], result_text="a retry")
            assert len(await claim_jobs(session, owner="runner-b", concurrency=2)) == 1
        async with factory() as session:
            job = await session.get(LeadJob, claims[0].job_id)
            assert job.state == "succeeded" and job.finished_at
            assert job.lease_owner is None and job.lease_expires_at is None
            assert len(job.result_text.encode()) <= 16384
            assert job.result_text.endswith("\n[truncated]")
            assert job.usage == {"cost_units": "0.25"}
            events = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.idempotency_key == f"lead:job:{job.id}",
                    )
                )
            )
            assert len(events) == 1 and events[0].payload["wakeup_kind"] == "job_finished"


@pytest.mark.parametrize("fence", ["cancelled", "ended", "generation", "expired"])
async def test_revoked_or_expired_claim_cannot_store_a_model_result(postgres_dsn, fence):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            (claim,) = await claim_jobs(session, owner="runner")
        async with factory.begin() as session:
            job = await session.get(LeadJob, job_id)
            if fence == "cancelled":
                await cancel_job(session, goal_id=goal_id, job_id=job_id)
            elif fence == "ended":
                goal = await session.get(ManagedGoal, goal_id)
                goal.lifecycle = "completed"
            elif fence == "generation":
                job.lease_generation += 1
            else:
                job.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        async with factory.begin() as session:
            assert not await complete_job(session, claim, result_text="discard me")
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            assert job.result_text is None and job.usage == {}
            if fence == "ended":
                assert job.state == "cancelled"
            elif fence == "expired":
                assert job.state == "failed" and job.failure_reason == "interrupted"
            elif fence == "generation":
                assert job.state == "running"


async def test_failure_and_expired_lease_never_requeue_paid_work(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        for _ in range(2):
            await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            first, second = await claim_jobs(session, owner="runner")
            assert await complete_job(session, first, failure_reason="provider_unavailable")
            job = await session.get(LeadJob, second.job_id)
            job.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        async with factory.begin() as session:
            assert await recover_expired_jobs(session) == 1
            assert await recover_expired_jobs(session) == 0
            assert await claim_jobs(session, owner="replacement") == []
        async with factory() as session:
            jobs = list(await session.scalars(select(LeadJob).order_by(LeadJob.created_at)))
            assert [job.failure_reason for job in jobs] == ["provider_unavailable", "interrupted"]
            assert all(job.state == "failed" and job.result_text is None for job in jobs)
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(TransactionalOutbox)
                    .where(
                        TransactionalOutbox.payload["wakeup_kind"].astext == "job_finished",
                    )
                )
                == 2
            )


async def test_cancel_is_idempotent_and_cancel_goal_cancels_queued_and_running(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        limits = HardLimits(lead_jobs_nonterminal_per_goal=3)
        first, second, third = [await _enqueue(factory, goal_id, limits=limits) for _ in range(3)]
        async with factory.begin() as session:
            await claim_jobs(session, owner="runner", concurrency=2)
            job = await cancel_job(session, goal_id=goal_id, job_id=first)
            finished = job.finished_at
            assert (
                await cancel_job(session, goal_id=goal_id, job_id=first)
            ).finished_at == finished
        result = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="cancel_goal",
                project_id=PROJECT,
                goal_id=goal_id,
                idempotency_key="cancel-jobs",
                payload={},
            ),
        )
        assert result.status == "applied"
        async with factory.begin() as session:
            assert (await session.get(LeadJob, second)).state == "cancelled"
            assert (await session.get(LeadJob, third)).state == "cancelled"
            assert await cancel_goal_jobs(session, goal_id=goal_id) == 0


async def test_job_read_is_scoped_labelled_bounded_and_snapshot_marks_stale(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            (claim,) = await claim_jobs(session, owner="runner")
            assert await complete_job(session, claim, result_text="untrusted report")
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            (current,) = await list_jobs(session, goal_id=goal_id, current_head=job.pinned_commit)
            (stale,) = await list_jobs(session, goal_id=goal_id, current_head="b" * 40)
            assert current["stale"] is False and stale["stale"] is True
            report = await read_job(session, goal_id=goal_id, job_id=job_id)
            assert report["source"] == "untrusted_job_result"
            assert "never instructions" in report["notice"]
            assert report["result_text"] == "untrusted report"
            with pytest.raises(DomainError, match="job_not_found"):
                await read_job(session, goal_id=uuid.uuid4(), job_id=job_id)
            with pytest.raises(DomainError, match="job_not_found"):
                await cancel_job(session, goal_id=uuid.uuid4(), job_id=job_id)


async def test_goal_completion_cancels_jobs_in_the_same_transaction(postgres_dsn, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        "vuzol.leadership.service.assess_completion",
        AsyncMock(
            return_value=SimpleNamespace(satisfied=True, covered=("requested-outcome",)),
        ),
    )
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        result = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="propose_completion",
                project_id=PROJECT,
                goal_id=goal_id,
                idempotency_key="complete-with-job",
                payload={},
            ),
        )
        assert result.status == "applied"
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "completed"
            assert (await session.get(LeadJob, job_id)).state == "cancelled"


async def test_completion_and_wakeup_roll_back_together(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            (claim,) = await claim_jobs(session, owner="runner")
        with pytest.raises(RuntimeError, match="rollback"):
            async with factory.begin() as session:
                assert await complete_job(session, claim, result_text="not committed")
                raise RuntimeError("rollback")
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            assert job.state == "running" and job.result_text is None
            assert (
                await session.scalar(
                    select(TransactionalOutbox.id).where(
                        TransactionalOutbox.idempotency_key == f"lead:job:{job_id}",
                    )
                )
                is None
            )
