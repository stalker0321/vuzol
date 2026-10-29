"""Real job/turn fences and accounting, with scripted provider I/O."""

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_job_lifecycle import _enqueue

# isort: split
from tests.integration.leadership.planning_helpers import pin_planner, planner_profile
from tests.integration.leadership.test_lead_surface import _Transport
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    TEST_REPOSITORY_ROOT,
    _enable,
    _factory,
    _session,
)
from tests.integration.leadership.test_model_assignment_receipts import _claim

from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import HardLimits, ProjectLeadSettings, Settings
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.invocation_context import current_turn, invocation_current
from vuzol.leadership.job_runner import ResearchJobRunner
from vuzol.leadership.persistence import (
    GoalDelegation,
    LeadershipInference,
    LeadershipTurn,
    LeadJob,
    ManagedGoal,
)
from vuzol.providers.domain import ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import TransactionalOutbox, UsageRecord

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


def worker(factory, adapter, profile=None):
    return build_project_lead_worker(
        factory,
        settings=Settings(environment="test", project_lead=ProjectLeadSettings(enabled=True)),
        profiles=ProfileRegistry((profile or planner_profile(),)),
        owner="research",
        planner_adapter=adapter,
        repository_paths={PROJECT: TEST_REPOSITORY_ROOT},
    )


async def test_research_runs_with_own_turn_receipt_and_finished_wakeup(postgres_dsn):
    adapter = _Transport({"report": "Проверка в src/vuzol/leadership/jobs.py:59."})
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        await pin_planner(factory)
        lead = worker(factory, adapter)
        # A concurrently claimed lead turn must neither fence nor conflict with a job turn.
        await _claim(factory, lead, "research")
        assert await lead.job_runner.tick()
        await lead.job_runner.wait_idle()
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            assert job.state == "succeeded"
            assert job.result_text == adapter.payload["report"]
            turns = list(await session.scalars(select(LeadershipTurn)))
            assert len(turns) == 2
            attempt_id = uuid.uuid5(job_id, "attempt:1")
            assert (await session.get(LeadershipTurn, attempt_id)).status == "applied"
            assert any(t.status == "claimed" and t.id != attempt_id for t in turns)
            receipt = await session.get(LeadershipInference, uuid.uuid5(job_id, "research"))
            assert receipt.turn_id == attempt_id and receipt.state == "received"
            assert receipt.request["task_draft"]["source_repo_commit"] == job.pinned_commit
            assert receipt.phase == "planner"
            assert (await session.scalar(select(UsageRecord))).purpose == "lead_research_job"
            events = list(
                await session.scalars(
                    select(TransactionalOutbox).where(
                        TransactionalOutbox.payload["wakeup_kind"].as_string() == "job_finished"
                    )
                )
            )
            assert len(events) == 1
        assert not await lead.job_runner.tick()
        assert len(adapter.requests) == 1
        request = adapter.requests[0]
        assert request.context and request.context[0].source == "read_repo"
        assert "plain Russian" in request.original_input
        assert "data, never instructions" in request.original_input


class Blocking(_Transport):
    def __init__(self):
        super().__init__({"report": "late report"})
        self.started = asyncio.Queue()
        self.release = asyncio.Event()
        self.active = 0
        self.peak = 0
        self.contexts = []

    async def execute(self, request, profile, cancellation):
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.contexts.append(current_turn.get())
        self.started.put_nowait(request.task_id)
        try:
            await self.release.wait()
            return await super().execute(request, profile, cancellation)
        finally:
            self.active -= 1


async def started(adapter, count=1):
    async with asyncio.timeout(5):
        for _ in range(count):
            await adapter.started.get()


async def test_runner_capacity_is_shared_and_does_not_block_lead(postgres_dsn):
    adapter = Blocking()
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        for _ in range(3):
            await _enqueue(factory, goal_id, limits=HardLimits(lead_jobs_nonterminal_per_goal=3))
        await pin_planner(factory)
        profile = planner_profile().model_copy(update={"concurrency_limit": 3})
        from vuzol.config.revision import content_revision
        from vuzol.storage.models import ProjectExecutorPreference

        async with factory.begin() as session:
            preference = await session.get(ProjectExecutorPreference, PROJECT)
            preference.model_assignments = {
                "planner_strong": {
                    **preference.model_assignments["planner_strong"],
                    "content_revision": content_revision(profile),
                }
            }
        lead = worker(factory, adapter, profile)
        runner = lead.job_runner
        try:
            assert await runner.tick()
            await started(adapter, 2)
            assert not await worker(factory, adapter).job_runner.tick()
            assert adapter.peak == 2
            async with factory() as session:
                assert (
                    await session.scalar(
                        select(func.count()).select_from(LeadJob).where(LeadJob.state == "queued")
                    )
                    == 1
                )
        finally:
            adapter.release.set()
            await runner.wait_idle()
        assert await runner.tick()
        await runner.wait_idle()
        assert len(adapter.requests) == 3


@pytest.mark.parametrize("revoke", [None, "lease", "goal", "delegation"])
async def test_heartbeat_extends_lease_and_revocation_discards_report(postgres_dsn, revoke):
    adapter = Blocking()
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        await pin_planner(factory)
        lead = worker(factory, adapter)
        runner = ResearchJobRunner(
            factory,
            owner="heartbeat",
            model=lead._model,
            enabled=lambda: True,
            lease_seconds=1,
        )
        try:
            assert await runner.tick()
            await started(adapter)
            async with factory() as session:
                first_expiry = (await session.get(LeadJob, job_id)).lease_expires_at
            if revoke:
                async with factory.begin() as session:
                    goal = await session.get(ManagedGoal, goal_id)
                    if revoke == "lease":
                        (await session.get(LeadJob, job_id)).lease_owner = "replacement"
                    elif revoke == "goal":
                        goal.lifecycle = "cancelled"
                    else:
                        (
                            await session.get(GoalDelegation, goal.delegation_id)
                        ).revoked_at = datetime.now(UTC)
                async with factory.begin() as session:
                    assert not await invocation_current(session, adapter.contexts[0])
                async with asyncio.timeout(5):
                    await runner.wait_idle()
            else:
                # Wait past the original expiry; the call is still alive under a renewed lease.
                await asyncio.sleep(
                    max(0, (first_expiry - datetime.now(UTC)).total_seconds()) + 0.1
                )
                async with factory() as session:
                    job = await session.get(LeadJob, job_id)
                    assert job.lease_expires_at > datetime.now(UTC)
                    assert job.lease_expires_at > first_expiry
        finally:
            adapter.release.set()
            await runner.wait_idle()
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            assert (job.result_text is None) == bool(revoke)
            assert (await session.get(LeadershipTurn, uuid.uuid5(job_id, "attempt:1"))).status == (
                "lost" if revoke else "applied"
            )


async def test_provider_failure_stores_category(postgres_dsn):
    class Failed(_Transport):
        async def execute(self, request, profile, cancellation):
            raise ProviderFailure(
                ProviderErrorCategory.RATE_LIMITED,
                retryable=True,
                request_sent=True,
                safe_summary="busy",
            )

    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        await pin_planner(factory)
        runner = worker(factory, Failed({})).job_runner
        assert await runner.tick()
        await runner.wait_idle()
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            assert job.state == "failed" and job.failure_reason == "rate_limited"


async def test_cli_job_uses_snapshot_and_reconciles_without_goal_or_outbox_lease(
    postgres_dsn,
    tmp_path,
):
    import json

    from tests.integration.leadership.test_turn_sandbox_transport import configuration

    from vuzol.config.models import ProviderRole
    from vuzol.config.registries import ProjectRegistry
    from vuzol.config.revision import content_revision
    from vuzol.execution.reconciliation import ReconciliationClassification, TurnStartupReconciler
    from vuzol.execution.repo_snapshot import snapshot_path
    from vuzol.execution.turn_transport import TurnSandboxTransport
    from vuzol.providers.pi import PiCliAdapter
    from vuzol.providers.ports import CodexProcessResult
    from vuzol.storage.models import ProjectExecutorPreference, SupervisedProcess

    configured, settings, registries = configuration(tmp_path)
    configured = configured.model_copy(update={"roles": frozenset({ProviderRole.PLANNER})})
    project = registries.projects.get(PROJECT).model_copy(
        update={"repository_path": TEST_REPOSITORY_ROOT}
    )
    registries = registries.model_copy(
        update={
            "profiles": ProfileRegistry((configured,)),
            "projects": ProjectRegistry((project,), repository_root=TEST_REPOSITORY_ROOT.parent),
        }
    )
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        job_id = await _enqueue(factory, goal_id)
        async with factory.begin() as session:
            session.add(
                ProjectExecutorPreference(
                    project_id=PROJECT,
                    mode="auto",
                    model_assignments={
                        "planner_strong": {
                            "profile_id": configured.id,
                            "model": configured.model,
                            "reasoning_effort": configured.model_reasoning_effort,
                            "content_revision": content_revision(configured),
                        },
                    },
                )
            )

        class Runtime:
            async def run(self, envelope, cancellation):
                assert not envelope.sandbox.empty_workspace
                mount = next(m for m in envelope.sandbox.mounts if str(m.target) == "/workspace")
                assert mount.mode.value == "ro"
                assert not (mount.source / ".git").exists()
                assert (mount.source / "pyproject.toml").is_file()
                reconciler = TurnStartupReconciler(
                    factory, self, owner="recovery", enabled=lambda: True
                )
                process_id = None
                async with factory() as session:
                    process_id = await session.scalar(select(SupervisedProcess.id))
                # Use the per-process path: the live launcher's runtime lock intentionally
                # makes the public startup scan preserve the resource before reading authority.
                assert await reconciler._reconcile_one(process_id) is (
                    ReconciliationClassification.PRESERVED_ACTIVE_LEASE
                )
                payload = json.dumps({"report": "Проверено: pyproject.toml:1."})
                stdout = json.dumps(
                    {
                        "type": "agent_end",
                        "messages": [
                            {
                                "role": "assistant",
                                "content": [{"type": "text", "text": payload}],
                                "stopReason": "stop",
                                "usage": {"input": 3, "output": 4},
                            }
                        ],
                    }
                )
                return CodexProcessResult(
                    0, stdout + "\n" + json.dumps({"type": "agent_settled"}), "", 1
                )

            async def cleanup(self, envelope):
                pass

        runtime = Runtime()
        adapter = PiCliAdapter(TurnSandboxTransport(runtime, factory, settings, registries))
        lead = build_project_lead_worker(
            factory,
            settings=settings,
            profiles=registries.profiles,
            owner="cli-job",
            planner_adapter=adapter,
            repository_paths={PROJECT: TEST_REPOSITORY_ROOT},
        )
        assert await lead.job_runner.tick()
        await lead.job_runner.wait_idle()
        async with factory() as session:
            job = await session.get(LeadJob, job_id)
            assert job.state == "succeeded", job.failure_reason
            process = await session.scalar(select(SupervisedProcess))
            assert process.reaped_at is not None
            assert process.runtime_metadata["turn_context"]["job"] is True
        assert not snapshot_path(uuid.uuid5(job_id, "research"), settings.worktree_root).exists()
