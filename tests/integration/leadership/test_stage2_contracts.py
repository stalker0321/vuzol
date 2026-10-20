"""Fault boundaries not covered by the first review's five probes."""

import asyncio
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_lead_review_regressions import StartModel, control
from tests.integration.leadership.test_managed_goal_flow import (
    OWNER,
    PROJECT,
    _enable,
    _factory,
    _lead_service,
    _package_count,
    _runtime,
    _session,
    _worker,
    run_managed_goal_flow,
)

from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.model import LeadDecision, LeadModelUnavailable
from vuzol.leadership.persistence import LeadershipTurn, ManagedGoal
from vuzol.storage.models import Task, TransactionalOutbox, WorkPackage
from vuzol.storage.types import DeliveryStatus, TaskStatus, WorkPackageStatus
from vuzol.workflows.application import Principal
from vuzol.workflows.dispatch import WorkflowDispatcher

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def expire_claim(factory):
    """Fault injection only: advance both lease deadlines into the past."""
    async with factory.begin() as session:
        goal = await session.scalar(select(ManagedGoal))
        goal.lease_until = datetime.now(UTC) - timedelta(seconds=1)
        item = await session.scalar(
            select(TransactionalOutbox).where(
                TransactionalOutbox.destination == "project_lead",
                TransactionalOutbox.status == DeliveryStatus.LEASED,
            )
        )
        item.lease_expires_at = goal.lease_until


async def test_resume_restores_the_existing_package_and_pending_task(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        async with factory() as session:
            original = await session.scalar(select(Task))
            previous = original.status
        assert (await control(factory, goal_id, "pause_goal")).status == "applied"
        async with factory() as session:
            task = await session.scalar(select(Task))
            assert task.status is TaskStatus.PAUSED
        assert (await control(factory, goal_id, "resume_goal")).status == "applied"
        async with factory() as session:
            package = await session.scalar(select(WorkPackage))
            task = await session.scalar(select(Task))
            assert package.status is WorkPackageStatus.RUNNING
            assert task.status is previous
            assert await session.scalar(select(func.count()).select_from(Task)) == 1


async def test_resume_cannot_restore_a_revoked_grant(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await control(factory, goal_id, "revoke_delegation")
        result = await control(factory, goal_id, "resume_goal")
        assert result.status == "denied"
        assert result.reason == "delegation_inactive"


async def test_model_error_after_pause_does_not_unpause_the_goal(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class PausedFailure:
            async def decide(self, snapshot):
                await control(factory, goal_id, "pause_goal")
                raise LeadModelUnavailable("lead_model_unavailable")

        await _worker(factory, PausedFailure()).process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.lifecycle == "paused"
            assert goal.waiting_reason == "owner_pause"


async def test_expired_response_is_discarded_and_reclaimed(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))

        class Expiring:
            async def decide(self, snapshot):
                await expire_claim(factory)
                return await StartModel().decide(snapshot)

        await _worker(factory, Expiring(), owner="expired").process_one()
        assert await _package_count(factory) == 0
        await _worker(factory, StartModel(), owner="replacement").process_one()
        assert await _package_count(factory) == 1


@pytest.mark.parametrize("same_owner", [False, True])
@pytest.mark.parametrize("failed_response", [False, True])
async def test_old_response_cannot_overwrite_a_replacement_still_in_inference(
    postgres_dsn, same_owner, failed_response
):
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        first_ready, second_ready, release_first, release_second = (
            asyncio.Event() for _ in range(4)
        )

        class First:
            async def decide(self, snapshot):
                first_ready.set()
                await release_first.wait()
                if failed_response:
                    raise LeadModelUnavailable("lead_model_unavailable")
                return await StartModel().decide(snapshot)

        class Second:
            async def decide(self, snapshot):
                second_ready.set()
                await release_second.wait()
                return LeadDecision()

        first = asyncio.create_task(_worker(factory, First(), owner="one").process_one())
        await asyncio.wait_for(first_ready.wait(), 5)
        await expire_claim(factory)
        second = asyncio.create_task(
            _worker(factory, Second(), owner="one" if same_owner else "two").process_one()
        )
        try:
            await asyncio.wait_for(second_ready.wait(), 5)
            release_first.set()
            await asyncio.wait_for(first, 5)
            assert await _package_count(factory) == 0
            async with factory() as session:
                turn = await session.scalar(select(LeadershipTurn))
                assert turn.status == "claimed"
                assert turn.decision is None
            release_second.set()
            await asyncio.wait_for(second, 5)
            assert await _package_count(factory) == 0
        finally:
            release_first.set()
            release_second.set()
            await asyncio.gather(first, second, return_exceptions=True)


async def test_old_ack_does_not_release_a_successor_lease_with_the_same_worker_name(
    postgres_dsn, monkeypatch
):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        ack_ready, second_ready, release_ack, release_second = (asyncio.Event() for _ in range(4))

        class Idle:
            async def decide(self, snapshot):
                return LeadDecision()

        class Second:
            async def decide(self, snapshot):
                second_ready.set()
                await release_second.wait()
                return LeadDecision()

        worker = _worker(factory, Idle(), owner="same-worker")
        original_ack = worker._ack

        async def delayed_ack(*args, **kwargs):
            ack_ready.set()
            await release_ack.wait()
            await original_ack(*args, **kwargs)

        monkeypatch.setattr(worker, "_ack", delayed_ack)
        first = asyncio.create_task(worker.process_one())
        second = None
        try:
            await asyncio.wait_for(ack_ready.wait(), 5)
            await control(factory, goal_id, "pause_goal")
            await control(factory, goal_id, "resume_goal")
            second = asyncio.create_task(
                _worker(factory, Second(), owner="same-worker").process_one()
            )
            await asyncio.wait_for(second_ready.wait(), 5)
            release_ack.set()
            await asyncio.wait_for(first, 5)
            async with factory() as session:
                goal = await session.get(ManagedGoal, goal_id)
                assert goal.lease_owner == "same-worker"
                assert goal.lease_epoch == 2
                assert goal.lease_until > datetime.now(UTC)
            release_second.set()
            await asyncio.wait_for(second, 5)
        finally:
            release_ack.set()
            release_second.set()
            await asyncio.gather(
                *([first] if second is None else [first, second]), return_exceptions=True
            )


async def test_wait_rejects_unknown_or_foreign_package(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        result = await _lead_service(factory).apply_agent(
            f"agent:project-lead:{PROJECT}",
            "worker",
            LeadCommand(
                name="wait_for",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=1,
                idempotency_key="bad-wait",
                payload={"condition": {"kind": "package", "package_id": str(uuid.uuid4())}},
            ),
        )
        assert result.status == "denied"
        assert result.reason == "wait_package_out_of_scope"
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).lifecycle == "active"


async def test_old_question_answer_cannot_answer_a_new_question(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        service = _lead_service(factory)
        for version, question in [(1, "First question"), (2, "Replacement question")]:
            asked = await service.apply_agent(
                f"agent:project-lead:{PROJECT}",
                "worker",
                LeadCommand(
                    name="request_permission",
                    project_id=PROJECT,
                    goal_id=goal_id,
                    expected_version=version,
                    idempotency_key=f"question-{version}",
                    payload={"question": question},
                ),
            )
            assert asked.status == "waiting"
        result = await service.apply_human(
            Principal(OWNER, "telegram"),
            LeadCommand(
                name="answer_permission",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=3,
                idempotency_key="late-answer",
                payload={"question_version": 2, "answer": "Yes"},
            ),
        )
        assert result.status == "stale"
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            assert goal.wakeup_condition["question"] == "Replacement question"
            assert "owner_answer" not in goal.constraints


@pytest.mark.parametrize("action", ["revoke_delegation", "cancel_goal"])
async def test_owner_control_after_real_approval_prevents_the_git_effect(
    postgres_dsn, tmp_path, monkeypatch, action
):
    await run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch, control_before_apply=action)


async def test_pending_dispatch_does_not_restart_a_paused_goal(postgres_dsn, tmp_path, monkeypatch):
    from vuzol.storage.models import Run

    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=True)
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        await control(factory, goal_id, "pause_goal")
        dispatcher = WorkflowDispatcher(runtime, factory, owner="paused-dispatch")
        assert await dispatcher.process_one()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Run)) == 0
            task = await session.scalar(select(Task))
            assert task.status is TaskStatus.PAUSED
            item = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.destination == "workflow_dispatch"
                )
            )
            assert item.status is DeliveryStatus.PENDING


async def test_copying_oauth_words_into_committed_readme_is_not_evidence(
    postgres_dsn, tmp_path, monkeypatch
):
    import subprocess

    from tests.integration.leadership import test_managed_goal_flow as flow

    original = flow._git_init

    def with_claim(repository):
        original(repository)
        (repository / "README.md").write_text(
            "Implement working OAuth login\nTODO: implement it.\n"
        )
        subprocess.run(["git", "-C", str(repository), "add", "README.md"], check=True)
        subprocess.run(
            ["git", "-C", str(repository), "commit", "-m", "claim only"],
            check=True,
            capture_output=True,
        )
        return subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
        ).strip()

    monkeypatch.setattr(flow, "_git_init", with_claim)
    await run_managed_goal_flow(
        postgres_dsn,
        tmp_path,
        monkeypatch,
        request="Implement working OAuth login",
        expected_completed=False,
    )
    async with _factory(postgres_dsn) as factory, factory() as session:
        from vuzol.leadership.persistence import ManagedGoalEvidence

        goal = await session.scalar(select(ManagedGoal))
        assert goal.lifecycle == "waiting"
        rows = list(await session.scalars(select(ManagedGoalEvidence)))
        assert rows and all(row.satisfied is False for row in rows)
        assert all(row.checker == "goal_review_v2" for row in rows)


async def test_crash_after_first_command_resumes_from_receipts(postgres_dsn, monkeypatch):
    from vuzol.leadership.persistence import LeadershipCommandReceipt
    from vuzol.leadership.service import LeadershipCommandService

    original = LeadershipCommandService.apply_agent
    crashed = False

    async def crash_after_commit(self, principal, source, command, **kwargs):
        nonlocal crashed
        result = await original(self, principal, source, command, **kwargs)
        if not crashed and command.name == "propose_batch":
            crashed = True
            raise RuntimeError("after first command commit")
        return result

    monkeypatch.setattr(LeadershipCommandService, "apply_agent", crash_after_commit)
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        worker = _worker(factory, StartModel())
        with pytest.raises(RuntimeError, match="after first command commit"):
            await worker.process_one()
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).version == 2
        assert await _package_count(factory) == 0
        await worker.process_one()
        async with factory() as session:
            assert (await session.get(ManagedGoal, goal_id)).version == 3
            rows = list(
                await session.scalars(
                    select(LeadershipCommandReceipt).where(
                        LeadershipCommandReceipt.actor_kind == "agent"
                    )
                )
            )
            assert sorted(row.command_name for row in rows) == ["propose_batch", "start_batch"]
            assert sorted(row.expected_version for row in rows) == [1, 2]
            turn = await session.scalar(select(LeadershipTurn))
            assert [item["expected_version"] for item in turn.decision["commands"]] == [1, 2]
        assert await _package_count(factory) == 1


async def test_slow_inference_renews_both_leases(postgres_dsn):
    from vuzol.leadership.worker import ProjectLeadWorker

    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))

        class Slow:
            async def decide(self, snapshot):
                await asyncio.sleep(1.2)
                assert not await _worker(factory, StartModel(), owner="other").process_one()
                return await StartModel().decide(snapshot)

        worker = ProjectLeadWorker(
            factory, owner="slow", model=Slow(), enabled=lambda: True, lease_seconds=1
        )
        assert await worker.process_one()
        assert await _package_count(factory) == 1
        async with factory() as session:
            assert (await session.scalar(select(ManagedGoal))).lease_epoch == 1


async def test_old_evidence_does_not_cover_changed_contract_or_result(
    postgres_dsn, tmp_path, monkeypatch
):
    from vuzol.leadership.evidence import assess_completion

    await run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    async with _factory(postgres_dsn) as factory:
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            assert (await assess_completion(session, goal)).satisfied
            goal.scope_revision += 1
            assert not (await assess_completion(session, goal)).satisfied
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            goal.criteria = [{"id": "requested-outcome", "description": "Working OAuth login"}]
            assert not (await assess_completion(session, goal)).satisfied
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            package = await session.get(WorkPackage, goal.current_package_id)
            package.integration_head_commit = "e" * 40
            assert not (await assess_completion(session, goal)).satisfied
        async with factory() as session:
            goal = await session.scalar(select(ManagedGoal))
            package = await session.get(WorkPackage, goal.current_package_id)
            package.status = WorkPackageStatus.RUNNING
            assert not (await assess_completion(session, goal)).satisfied
        async with factory() as session:
            import subprocess

            from vuzol.execution.git import LocalGit

            goal = await session.scalar(select(ManagedGoal))
            repository = tmp_path / "repositories" / PROJECT
            assert (await assess_completion(session, goal, check_live_head=True)).satisfied
            base = await LocalGit().resolve_commit(repository, "main")
            await asyncio.to_thread(
                subprocess.run,
                [
                    "git",
                    "-C",
                    str(repository),
                    "update-ref",
                    f"refs/heads/{goal.integration_branch}",
                    base,
                ],
                check=True,
            )
            assert not (await assess_completion(session, goal, check_live_head=True)).satisfied


async def test_concurrent_enable_with_same_command_has_one_receipt(postgres_dsn):
    from tests.integration.leadership.test_command_boundary import _enable as enable_command

    from vuzol.leadership.persistence import LeadershipCommandReceipt

    async with _factory(postgres_dsn) as factory:
        session_id = await _session(factory)
        command = replace(enable_command(session_id), project_id=PROJECT)
        service = _lead_service(factory)
        results = await asyncio.gather(
            *[service.apply_human(Principal(OWNER, "telegram"), command) for _ in range(2)]
        )
        assert sorted(item.status for item in results) == ["applied", "duplicate"]
        assert results[0].receipt_id == results[1].receipt_id
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(LeadershipCommandReceipt))
                == 1
            )


async def test_enable_does_not_take_over_a_legacy_package(postgres_dsn):
    from tests.integration.leadership.test_command_boundary import _enable as enable_command
    from tests.integration.storage.test_discussion_application import create_package

    async with _factory(postgres_dsn) as factory:
        session_id, package_id, _, _ = await create_package(factory)
        result = await _lead_service(factory).apply_human(
            Principal(OWNER, "telegram"), replace(enable_command(session_id), project_id=PROJECT)
        )
        assert result.status == "denied"
        assert result.reason == "package_already_live"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
            assert (await session.get(WorkPackage, package_id)).status is WorkPackageStatus.DRAFT
