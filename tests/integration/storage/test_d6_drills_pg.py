"""D6 drills on PostgreSQL (+ temp git): acceptance/memory, budgets, fences."""

from __future__ import annotations

import asyncio
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config.settings import HardLimits
from vuzol.discussion.domain import PlanDraft, PlanItemDraft
from vuzol.discussion.horizon import HORIZON_CONTRACT_ENABLED
from vuzol.discussion.memory_writer import MemoryWriterService
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.discussion.service import WorkPackageService
from vuzol.execution.git import GitError, LocalGit
from vuzol.providers.budgets import (
    AccountingContext,
    BudgetExceeded,
    estimate_reservation,
    reserve_invocation_budget,
    settle_invocation_budget,
    usage_retry_subtotal,
    usage_totals_by_purpose,
)
from vuzol.providers.domain import NormalizedUsage
from vuzol.storage.attempts import record_work_attempt, snapshot_task_spec
from vuzol.storage.leasing import claim_step
from vuzol.storage.migration_preflight import require_migration_head
from vuzol.storage.models import (
    MaterializationLink,
    PlanRevision,
    Step,
    Task,
    TaskSpecRevision,
    TransactionalOutbox,
    WorkAttempt,
    WorkPackage,
)
from vuzol.storage.types import (
    IdempotencyClass,
    PlanRevisionCreatedBy,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorkPackageStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.acceptance import record_evidence, record_waiver
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.service import commit_step_outcome

pytestmark = pytest.mark.postgresql


def _plan() -> PlanDraft:
    return PlanDraft(
        title="Horizon plan",
        items=tuple(
            PlanItemDraft(
                local_id=f"item-{ordinal}",
                summary=f"Step {ordinal}",
                goal=f"Goal {ordinal}",
                expected_outcome=f"Outcome {ordinal}",
                completion_criteria=(f"Check {ordinal}",),
                allowed_scope="src/**",
            )
            for ordinal in (1, 2)
        ),
    )


async def _horizon_package(factory: object) -> tuple[uuid.UUID, uuid.UUID]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    async with UnitOfWork(typed) as uow:
        session_id = await uow.discussions.create_session(
            project_id="vuzol", chat_id=-100, message_thread_id=10
        )
        service = WorkPackageService(uow)
        created = await service.create_draft(
            session_id=session_id,
            project_id="vuzol",
            plan=_plan(),
            created_by=PlanRevisionCreatedBy.PLANNER_MODEL,
            actor_type="planner_model",
            goal="ship the horizon",
            exit_criteria=[{"criterion_id": "done"}],
        )
        generation = await service.approve(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
        await WorkPackageSequencer(uow).start(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=generation,
            user_id=42,
            horizon_enabled=True,
        )
        return created.package_id, created.revision_id


async def _to_evaluating(factory: object, package_id: uuid.UUID) -> None:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    for _ in range(6):
        async with UnitOfWork(typed) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            if package.horizon_phase == "evaluating":
                return
            assert package.cursor_ordinal is not None
            link = await uow.session.scalar(
                select(MaterializationLink).where(
                    MaterializationLink.work_package_id == package_id,
                    MaterializationLink.ordinal == package.cursor_ordinal,
                )
            )
            assert link is not None
            task = await uow.session.get(Task, link.task_id, with_for_update=True)
            assert task is not None
            task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(
                task_id=link.task_id, horizon_enabled=True
            )
    raise AssertionError("package did not reach evaluating")


def test_acceptance_waiver_emits_durable_memory_job(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        package_id, revision_id = await _horizon_package(factory)
        await _to_evaluating(factory, package_id)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            service = WorkPackageService(uow)
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "b" * 40
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            waiver = await record_waiver(
                uow.session,
                package_id=package_id,
                integration_head="a" * 40,
                principal_user_id=7,
                reason="ship now, verify on prod",
            )
            await service.record_acceptance(
                package_id=package_id,
                revision_number=1,
                h8=revision.content_hash[:8],
                expected_status_generation=package.version,
                accepted=True,
                artifact_id=None,
                user_id=7,
                horizon_enabled=True,
                waiver_id=waiver.id,
            )
            assert package.status is WorkPackageStatus.COMPLETED
            jobs = tuple(
                (
                    await uow.session.scalars(
                        select(TransactionalOutbox).where(
                            TransactionalOutbox.destination == "memory_extract"
                        )
                    )
                ).all()
            )
            assert len(jobs) == 1
        writer = MemoryWriterService(factory, owner="test-memory")
        assert await writer.process_one() is True
        async with UnitOfWork(factory) as uow:
            from vuzol.discussion.memory_units import RecallQuery

            found = await uow.memory_units.recall(RecallQuery(project_id="vuzol"))
            assert len(found) == 1
            assert found[0].unit_type == "outcome_template"
            assert found[0].status.value == "verified"
            assert "waiver" in found[0].text
        await engine.dispose()

    asyncio.run(scenario())


async def _prove_promotion(factory: object, *, package_id: uuid.UUID) -> None:
    """Record a CONSUMED approval envelope for the last item (D2 proof pattern)."""

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from vuzol.storage.models import Approval as ApprovalRow
    from vuzol.storage.types import ApprovalStatus as ApprovalStatusRow

    assert isinstance(factory, async_sessionmaker)
    typed: async_sessionmaker[AsyncSession] = factory
    async with UnitOfWork(typed) as uow:
        assert uow.session is not None
        links = (
            await uow.session.scalars(
                select(MaterializationLink).where(MaterializationLink.work_package_id == package_id)
            )
        ).all()
        last = max(links, key=lambda link: link.ordinal or 0)
        run_id = await uow.runs.create(
            task_id=last.task_id,
            workflow_type="coding",
            workflow_version="4",
            budget_mode="balanced",
            configuration_revision="c" * 64,
            policy_revision="d" * 64,
            status=RunStatus.COMPLETED,
        )
        step_record = await uow.steps.create(
            run_id=run_id,
            ordinal=3,
            step_type="approval",
            idempotency_class=IdempotencyClass.IDEMPOTENT,
        )
        db_step = await uow.session.get(Step, step_record.id, with_for_update=True)
        assert db_step is not None
        db_step.status = StepStatus.COMPLETED
        db_step.payload = {
            **db_step.payload,
            "action_envelope": {"result_commit": "b" * 40},
        }
        uow.session.add(
            ApprovalRow(
                step_id=db_step.id,
                action_envelope_hash="dd" * 32,
                requested_action="apply_result",
                normalized_target="vuzol:main",
                human_summary="apply",
                token_hash=f"ee{uuid.uuid4().hex[:56]}",
                status=ApprovalStatusRow.CONSUMED,
                expires_at=datetime.now(UTC) + timedelta(days=1),
            )
        )


def test_acceptance_evidence_emits_memory_unit_with_refs(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        import json as _json

        from vuzol.execution.artifacts import ArtifactStore

        engine, factory = storage(postgres_dsn)
        package_id, revision_id = await _horizon_package(factory)
        await _to_evaluating(factory, package_id)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "b" * 40
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            links = (
                await uow.session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == package_id
                    )
                )
            ).all()
            task_id = links[0].task_id
            run_id = await uow.runs.create(
                task_id=task_id,
                workflow_type="coding",
                workflow_version="4",
                budget_mode="balanced",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
                status=RunStatus.RUNNING,
            )
            step_record = await uow.steps.create(
                run_id=run_id,
                ordinal=1,
                step_type="acceptance",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
            )
        store = ArtifactStore(
            tmp_path, max_bytes=1_000_000, retention_days=7, redaction_patterns=()
        )
        document = {
            "schema": "acceptance-evidence.v1",
            "package_id": str(package_id),
            "plan_revision_id": str(revision_id),
            "plan_content_hash": revision.content_hash,
            "goal": "ship the horizon",
            "goal_revision": 1,
            "spec_revision": None,
            "configuration_revision": "c" * 64,
            "policy_revision": "d" * 64,
            "integration_base_head": "a" * 40,
            "result_commit": "b" * 40,
            "criteria": [{"criterion_id": "done", "satisfied": True}],
            "test_results": [],
            "review_refs": ["aa" * 32],
            "unresolved_caveats": [],
            "unresolved_effects": [],
            "created_at": "2026-09-30T00:00:00+00:00",
        }
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            artifact = await store.persist(
                uow.session,
                task_id=task_id,
                run_id=run_id,
                step_id=step_record.id,
                artifact_type="acceptance_evidence",
                content=_json.dumps(document, sort_keys=True).encode(),
                media_type="application/json",
                sensitivity="internal",
                visibility="private",
            )
            artifact_id = artifact.id
            await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document=document,
                artifact_id=artifact_id,
            )
        from vuzol.discussion.service import WorkPackageService as _Service

        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            service = _Service(uow)
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            await _prove_promotion(factory, package_id=package_id)
            await service.record_acceptance(
                package_id=package_id,
                revision_number=1,
                h8=revision.content_hash[:8],
                expected_status_generation=package.version,
                accepted=True,
                artifact_id=artifact_id,
                user_id=7,
                horizon_enabled=True,
            )
            assert package.acceptance_artifact_id == artifact_id
        writer = MemoryWriterService(factory, owner="test-memory")
        assert await writer.process_one() is True
        async with UnitOfWork(factory) as uow:
            from vuzol.discussion.memory_units import RecallQuery

            found = await uow.memory_units.recall(RecallQuery(project_id="vuzol"))
            assert len(found) == 1
            assert found[0].source_artifact_id == artifact_id
            assert found[0].source_acceptance_evidence_id is not None
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


def test_exhaustion_blocks_spend_but_keeps_final_evidence(postgres_dsn: str) -> None:
    async def scenario() -> None:
        from vuzol.config import LaunchMode
        from vuzol.config.models import Capability, CostClass, ProviderProfileConfig
        from vuzol.workflows.acceptance import record_evidence

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
            # Final evidence is a durable DB record, not an unreserved paid check.
            package_id, _revision_id = await _horizon_package(factory)
            row = await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=None,
                document={
                    "schema": "acceptance-evidence.v1",
                    "package_id": str(package_id),
                    "plan_revision_id": str(_revision_id),
                    "plan_content_hash": "ab" * 32,
                    "goal": "ship the horizon",
                    "integration_base_head": "a" * 40,
                    "result_commit": "b" * 40,
                    "criteria": [{"criterion_id": "done", "satisfied": True}],
                    "test_results": [],
                    "review_refs": ["aa" * 32],
                },
                artifact_id=None,
            )
            assert row.package_id == package_id
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.anyio
async def test_migration_head_verified_on_postgres(postgres_dsn: str) -> None:
    engine, _factory = storage(postgres_dsn)
    try:
        await require_migration_head(engine)
    finally:
        await engine.dispose()


def test_pinned_contract_survives_flag_off_without_restart(postgres_dsn: str) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        package_id, _revision_id = await _horizon_package(factory)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None and package.status is WorkPackageStatus.RUNNING
            package.execution_contract_version = HORIZON_CONTRACT_ENABLED
            result = await WorkPackageSequencer(uow).materialize_running(
                package_id=package_id, horizon_enabled=False
            )
            assert result.task_id is not None
        await engine.dispose()

    asyncio.run(scenario())


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
