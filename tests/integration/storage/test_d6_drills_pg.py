"""D6 drills on PostgreSQL (+ temp git): acceptance/memory, budgets, fences."""

from __future__ import annotations

import asyncio
import subprocess
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import seed_task_run_step, storage
from tests.integration.usage_totals import usage_retry_subtotal, usage_totals_by_purpose
from vuzol.config.settings import HardLimits
from vuzol.execution.git import GitError, LocalGit
from vuzol.providers.budgets import (
    AccountingContext,
    BudgetExceeded,
    estimate_reservation,
    reserve_invocation_budget,
    settle_invocation_budget,
)
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.attempts import record_work_attempt, snapshot_task_spec
from vuzol.storage.leasing import claim_step
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.models import (
    Step,
    Task,
    TaskSpecRevision,
    WorkAttempt,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.service import commit_step_outcome

pytestmark = pytest.mark.postgresql


def test_exhaustion_blocks_spend(postgres_dsn: str) -> None:
    async def scenario() -> None:
        from vuzol.config import LaunchMode
        from vuzol.config.models import Capability, CostClass, ProviderProfileConfig

        engine, factory = storage(postgres_dsn)
        profile = ProviderProfileConfig(
            id="test-cheap",
            provider="test",
            model="cheap-1",
            launch_mode=LaunchMode.TOOL,
            credential_required=False,
            capabilities=frozenset({Capability.REPOSITORY_READ}),
            concurrency_limit=1,
            context_limit=10_000,
            output_limit=1_000,
            cost_class=CostClass.CHEAP,
            supported_task_types=frozenset({"general"}),
            input_cost_units_per_million=100.0,
            output_cost_units_per_million=200.0,
            quota_units_per_call=1.0,
        )
        tiny = HardLimits(task_cost_units=0.001)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.tasks.create(
                user_id=1, chat_id=-100, original_text="probe", task_type="general"
            )
            estimate = estimate_reservation(profile, input_tokens=1_000, output_tokens=500)
            with pytest.raises(BudgetExceeded):
                await reserve_invocation_budget(
                    uow.session,
                    invocation_id=uuid.uuid4(),
                    profile=profile,
                    estimate=estimate,
                    limits=tiny,
                    task_id=task.id,
                    accounting=AccountingContext(purpose="execute", pricing_revision="t1"),
                )
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.anyio
async def test_migration_head_verified_on_postgres(postgres_dsn: str) -> None:
    engine, _factory = storage(postgres_dsn)
    try:
        await require_migration_head(engine)
    finally:
        await engine.dispose()


def test_final_failure_leaves_target_branch_untouched(postgres_dsn: str, tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repository, check=True)
    (repository / "value.txt").write_text("base\n")
    subprocess.run(["git", "add", "value.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "base"], cwd=repository, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    (repository / "value.txt").write_text("advanced alone\n")
    subprocess.run(["git", "add", "value.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-m", "platform advanced alone"], cwd=repository, check=True)

    produced: list[str] = []

    async def scenario() -> None:
        git = LocalGit()
        worktree = tmp_path / "worktree"
        result = await _produce_result(git, repository, worktree, base)
        produced.append(result)
        with pytest.raises(GitError, match="target branch changed"):
            await git.apply_result(
                repository, worktree, target_branch="main", expected_head=base, result_commit=result
            )

    asyncio.run(scenario())
    head = subprocess.run(
        ["git", "rev-parse", "main"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    assert produced and head != produced[0]


def _git(cwd: Path, *args: str) -> str:
    out = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return out.stdout.strip()


async def _produce_result(git: LocalGit, repository: Path, worktree: Path, base: str) -> str:
    await git.add_worktree(repository, worktree, "result", base)
    (worktree / "value.txt").write_text("approved\n")
    _git(worktree, "add", "value.txt")
    _git(
        worktree,
        "-c",
        "user.email=test@example.com",
        "-c",
        "user.name=Test",
        "commit",
        "-m",
        "result",
    )
    return _git(worktree, "rev-parse", "HEAD")


def test_lease_and_revision_fences_compose(postgres_dsn: str) -> None:
    async def scenario() -> None:
        from vuzol.storage.errors import LeaseLost

        engine, factory = storage(postgres_dsn)
        task_record, _run_id, step_record = await seed_task_run_step(factory)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="worker-a",
                lease_seconds=60,
                capabilities=frozenset({"execute_code"}),
            )
            assert token is not None
            task = await session.get(Task, task_record.id, with_for_update=True)
            assert task is not None
            first_revision = await snapshot_task_spec(session, task)
            await snapshot_task_spec(session, task)
            # A concurrent worker steals the lease: the stale token fails closed.
            current = await session.get(Step, step_record.id, with_for_update=True)
            assert current is not None
            current.lease_generation += 1
            current.lease_owner = "worker-b"
            with pytest.raises(LeaseLost):
                await commit_step_outcome(
                    session,
                    token,
                    StepOutcome(kind=OutcomeKind.SUCCEEDED, result={}),
                )
            history = await session.scalars(
                select(TaskSpecRevision).where(TaskSpecRevision.task_id == task_record.id)
            )
            revisions = {row.spec_revision for row in history.all()}
            assert first_revision in revisions
        await engine.dispose()

    asyncio.run(scenario())


def test_attempt_history_is_append_only(postgres_dsn: str) -> None:
    async def scenario() -> None:

        engine, factory = storage(postgres_dsn)
        task_record, run_id, step_record = await seed_task_run_step(factory)
        async with factory.begin() as session:
            first = await record_work_attempt(
                session,
                task_id=task_record.id,
                run_id=run_id,
                step_id=step_record.id,
                attempt_kind="initial",
                purpose="coding",
            )
            second = await record_work_attempt(
                session,
                task_id=task_record.id,
                run_id=run_id,
                step_id=step_record.id,
                attempt_kind="retry",
                purpose="coding",
                parent_attempt_id=first.id,
            )
            current = await session.get(
                WorkAttempt,
                first.id,
            )
            assert current is not None
            assert current.attempt_kind == "initial"
            assert current.parent_attempt_id is None
            rows = (
                await session.scalars(
                    select(WorkAttempt).where(WorkAttempt.step_id == step_record.id)
                )
            ).all()
            assert {row.id for row in rows} == {first.id, second.id}
            assert second.parent_attempt_id == first.id
        await engine.dispose()

    asyncio.run(scenario())


def test_repair_replan_share_one_denominator(postgres_dsn: str) -> None:
    async def scenario() -> None:
        from vuzol.config import LaunchMode
        from vuzol.config.models import Capability, CostClass, ProviderProfileConfig

        engine, factory = storage(postgres_dsn)
        profile = ProviderProfileConfig(
            id="test-cheap",
            provider="test",
            model="cheap-1",
            launch_mode=LaunchMode.TOOL,
            credential_required=False,
            capabilities=frozenset({Capability.REPOSITORY_READ}),
            concurrency_limit=1,
            context_limit=10_000,
            output_limit=1_000,
            cost_class=CostClass.CHEAP,
            supported_task_types=frozenset({"general"}),
            input_cost_units_per_million=100.0,
            output_cost_units_per_million=200.0,
            quota_units_per_call=1.0,
        )
        limits = HardLimits()
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            first = await uow.tasks.create(
                user_id=1, chat_id=-100, original_text="probe", task_type="general"
            )
            for attempt_kind in ("initial", "retry"):
                estimate = estimate_reservation(profile, input_tokens=100, output_tokens=50)
                reservation = await reserve_invocation_budget(
                    uow.session,
                    invocation_id=uuid.uuid4(),
                    profile=profile,
                    estimate=estimate,
                    limits=limits,
                    task_id=first.id,
                    accounting=AccountingContext(
                        purpose="execute", attempt_kind=attempt_kind, pricing_revision="t1"
                    ),
                )
                await settle_invocation_budget(
                    uow.session,
                    reservation=reservation,
                    profile=profile,
                    usage=NormalizedUsage(input_tokens=100, output_tokens=50, duration_ms=5),
                    provider_request_id=f"req-{attempt_kind}",
                    outcome="succeeded",
                )
            totals = await usage_totals_by_purpose(uow.session, task_id=first.id)
            retry_cost, retry_count = await usage_retry_subtotal(uow.session, task_id=first.id)
            by_purpose = {purpose: (cost, count) for purpose, cost, count in totals}
            assert by_purpose["execute"][1] == 2
            # Retry rows are a projection of the same rows, never an addend.
            assert retry_count == 1
            assert retry_cost <= by_purpose["execute"][0]
        await engine.dispose()

    asyncio.run(scenario())
