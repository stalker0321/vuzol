"""WP05 integration: durable effect intent, reconciliation, cancel window."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from vuzol.config import DeliveryMode, GitDeliveryPolicy
from vuzol.execution.effect import (
    apply_operation_key,
    settle_applied,
)
from vuzol.execution.effect_reconciliation import EffectReconciler
from vuzol.execution.git import LocalGit
from vuzol.execution.result_apply import ResultApplyHandler
from vuzol.storage.models import Approval, Effect, Run, Step, Task, Worktree
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import (
    ApprovalStatus,
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash

from ..storage.helpers import storage

pytestmark = pytest.mark.postgresql


class CountingGit(LocalGit):
    def __init__(self) -> None:
        super().__init__()
        self.apply_calls = 0

    async def apply_result(self, *args: object, **kwargs: object) -> bool:
        self.apply_calls += 1
        return await super().apply_result(*args, **kwargs)  # type: ignore[arg-type]


def _registries(repository: Path) -> MagicMock:
    project = SimpleNamespace(
        enabled=True,
        default_branch="main",
        repository_path=repository,
        git_delivery=GitDeliveryPolicy(
            allowed_modes=frozenset({DeliveryMode.RETAIN, DeliveryMode.APPLY}),
            approval_required=frozenset({DeliveryMode.APPLY}),
        ),
    )
    registries = MagicMock(revision="c" * 64)
    registries.projects.get.return_value = project
    return registries


def _provision_repository(tmp_path: Path) -> tuple[Path, str]:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(("git", "init", "-b", "main", str(repository)), check=True)
    subprocess.run(("git", "-C", str(repository), "config", "user.name", "Test"), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "config", "user.email", "test@example.invalid"),
        check=True,
    )
    (repository / "value.txt").write_text("base\n")
    subprocess.run(("git", "-C", str(repository), "add", "."), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "commit", "-m", "base"), check=True, capture_output=True
    )
    base = subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(("git", "-C", str(repository), "switch", "--detach"), check=True)
    return repository, base


async def _seed(factory: object, tmp_path: Path) -> SimpleNamespace:
    repository, base = _provision_repository(tmp_path)

    git = LocalGit()
    worktree_path = tmp_path / "result-worktree"
    await git.add_worktree(repository, worktree_path, "step09a/test/result", base)
    (worktree_path / "value.txt").write_text("approved\n")
    await git.stage_paths(worktree_path, ("value.txt",))
    result_commit = await git.create_commit(worktree_path, "approved result")
    inspection = await git.inspect(worktree_path, base)
    identity, _remote = await git.repository_identity(repository)

    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    step_id = uuid.uuid4()
    approval_id = uuid.uuid4()
    envelope = {
        "schema_version": "result-approval.v1",
        "requested_action": "apply_result",
        "task_id": str(task_id),
        "run_id": str(run_id),
        "step_id": str(step_id),
        "project_id": "vuzol",
        "repository_identity_hash": identity,
        "target_branch": "main",
        "expected_target_head": base,
        "base_commit": base,
        "result_commit": result_commit,
        "diff_hash": inspection.diff_hash,
        "configuration_revision": "c" * 64,
        "policy_revision": "d" * 64,
    }
    token_hash = hashlib.sha256(f"{approval_id}:{envelope_hash(envelope)}".encode()).hexdigest()
    async with factory.begin() as session:  # type: ignore[attr-defined]
        task = Task(
            id=task_id,
            user_id=42,
            source_chat_id=-100,
            project_id="vuzol",
            original_text="bounded task",
            task_draft={"normalized_title": "Bounded task"},
            status=TaskStatus.EXECUTING,
            risk=RiskLevel.LOW,
            task_type="coding",
        )
        session.add(task)
        await session.flush()
        run = Run(
            id=run_id,
            task_id=task_id,
            workflow_type="coding",
            workflow_version="1",
            status=RunStatus.RUNNING,
            budget_mode="balanced",
            configuration_revision="c" * 64,
            policy_revision="d" * 64,
        )
        session.add(run)
        await session.flush()
        step = Step(
            id=step_id,
            run_id=run_id,
            ordinal=4,
            dependency_metadata={"predecessor_ordinals": [3]},
            step_type="approval",
            queue_class=QueueClass.PRIVILEGED,
            status=StepStatus.RUNNING,
            required_capabilities=["git"],
            payload={"approval_id": str(approval_id), "action_envelope": envelope},
            retry_class=RetryClass.NEVER,
            idempotency_class=IdempotencyClass.IDEMPOTENT,
            max_attempts=2,
            timeout_seconds=120,
            lease_owner="applier",
            lease_generation=1,
        )
        session.add(step)
        await session.flush()
        approval = Approval(
            id=approval_id,
            step_id=step_id,
            action_envelope_hash=envelope_hash(envelope),
            requested_action="apply_result",
            normalized_target="vuzol:main",
            human_summary="reviewed change",
            token_hash=token_hash,
            status=ApprovalStatus.APPROVED,
            expires_at=datetime.now(UTC) + timedelta(days=1),
        )
        worktree = Worktree(
            task_id=task_id,
            run_id=run_id,
            project_id="vuzol",
            repository_identity_hash=identity,
            base_commit=base,
            default_branch="main",
            expected_target_head=base,
            branch="step09a/test/result",
            path=str(worktree_path),
            owner="test",
            delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
            result_commit=result_commit,
            diff_hash=inspection.diff_hash,
            retention_until=datetime.now(UTC) + timedelta(days=1),
        )
        session.add_all((approval, worktree))
        await session.flush()
        worktree_id = worktree.id
    return SimpleNamespace(
        repository=repository,
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        approval_id=approval_id,
        worktree_id=worktree_id,
        base=base,
        result_commit=result_commit,
        envelope=envelope,
        operation_key=apply_operation_key(
            approval_id=approval_id, result_commit=result_commit, target_branch="main"
        ),
    )


def _request(seed: SimpleNamespace) -> StepExecutionRequest:
    return StepExecutionRequest(
        task_id=seed.task_id,
        run_id=seed.run_id,
        step_id=seed.step_id,
        step_type="approval",
        payload={"approval_id": str(seed.approval_id)},
        timeout_seconds=120,
        lease=LeaseToken(
            step=StepRecord(
                id=seed.step_id,
                run_id=seed.run_id,
                status=StepStatus.RUNNING,
                lease_generation=1,
                lease_owner="applier",
                lease_expires_at=None,
            ),
            owner="applier",
            generation=1,
        ),
    )


def test_apply_records_intent_and_settles_idempotently(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        seed = await _seed(factory, tmp_path)
        git = CountingGit()
        handler = ResultApplyHandler(factory, _registries(seed.repository), git)

        outcome = await handler.execute(_request(seed), CancellationContext())
        assert outcome.kind is OutcomeKind.SUCCEEDED
        assert git.apply_calls == 1
        assert await git.resolve_commit(seed.repository, "main") == seed.result_commit

        async with factory() as session:
            effect = await session.scalar(
                select(Effect).where(Effect.operation_key == seed.operation_key)
            )
            worktree = await session.get(Worktree, seed.worktree_id)
            approval = await session.get(Approval, seed.approval_id)
            assert effect is not None
            assert effect.status == "settled"
            assert effect.receipt_status == "applied"
            assert effect.reconcile_status == "confirmed"
            assert effect.lease_generation == 1
            assert worktree is not None and worktree.delivery_state is WorktreeDeliveryState.APPLIED
            assert approval is not None and approval.status is ApprovalStatus.CONSUMED

        # Duplicate completion: no second ref move, one effect row, still applied.
        replay = await handler.execute(_request(seed), CancellationContext())
        assert replay.kind is OutcomeKind.SUCCEEDED
        assert git.apply_calls == 2  # called again but idempotent no-op
        assert await git.resolve_commit(seed.repository, "main") == seed.result_commit
        async with factory() as session:
            effects = list((await session.scalars(select(Effect))).all())
            assert len(effects) == 1 and effects[0].status == "settled"
        await engine.dispose()

    asyncio.run(scenario())


def test_cancel_window_between_cas_and_record_is_settled_applied(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        seed = await _seed(factory, tmp_path)
        git = CountingGit()
        registries = _registries(seed.repository)
        handler = ResultApplyHandler(factory, registries, git)
        request = _request(seed)

        # Reproduce the window: intent recorded, CAS applied, business record lost
        # (the process is killed / cancelled before _record_applied commits).
        approval_id, envelope, worktree = await handler._load(request)
        await handler._record_intent(
            request, approval_id=approval_id, envelope=envelope, worktree=worktree
        )
        await git.apply_result(
            seed.repository,
            Path(worktree.path),
            target_branch=envelope["target_branch"],
            expected_head=envelope["expected_target_head"],
            result_commit=envelope["result_commit"],
        )
        async with factory.begin() as session:
            cancelled = await session.get(Step, seed.step_id, with_for_update=True)
            assert cancelled is not None
            cancelled.status = StepStatus.CANCELLED
            cancelled.unknown_effects = True
            cancelled.lease_owner = None
            cancelled.lease_expires_at = None
        async with factory() as session:
            pending = await session.scalar(
                select(Effect).where(Effect.operation_key == seed.operation_key)
            )
            assert pending is not None and pending.status == "dispatched"

        report = await EffectReconciler(
            factory, git, registries, owner="test-reconciler"
        ).reconcile_startup()
        assert report.lock_acquired
        assert report.confirmed_count == 1
        assert await git.resolve_commit(seed.repository, "main") == seed.result_commit
        async with factory() as session:
            effect = await session.scalar(
                select(Effect).where(Effect.operation_key == seed.operation_key)
            )
            worktree_row = await session.get(Worktree, seed.worktree_id)
            approval = await session.get(Approval, seed.approval_id)
            assert effect is not None
            assert effect.status == "settled" and effect.receipt_status == "applied"
            assert effect.reconcile_method == "read_git_ref"
            assert worktree_row is not None
            assert worktree_row.delivery_state is WorktreeDeliveryState.APPLIED
            assert approval is not None and approval.status is ApprovalStatus.CONSUMED
        await engine.dispose()

    asyncio.run(scenario())


def test_not_applied_observation_denies_without_touching_business_state(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        seed = await _seed(factory, tmp_path)
        git = LocalGit()
        registries = _registries(seed.repository)
        async with factory.begin() as session:
            session.add(
                Effect(
                    schema_version="effect.v1",
                    operation_key=seed.operation_key,
                    step_id=seed.step_id,
                    task_id=seed.task_id,
                    run_id=seed.run_id,
                    effect_class="isolated_mutation",
                    target_kind="git_ref",
                    target_reference="refs/heads/main",
                    idempotency="reconcilable",
                    payload_hash="a" * 64,
                    lease_generation=1,
                    status="dispatched",
                    context={
                        "project_id": "vuzol",
                        "worktree_id": str(seed.worktree_id),
                        "target_branch": "main",
                        "expected_head": seed.base,
                        "result_commit": seed.result_commit,
                    },
                )
            )
        report = await EffectReconciler(
            factory, git, registries, owner="test-reconciler"
        ).reconcile_startup()
        assert report.denied_count == 1
        async with factory() as session:
            effect = await session.scalar(
                select(Effect).where(Effect.operation_key == seed.operation_key)
            )
            worktree = await session.get(Worktree, seed.worktree_id)
            assert effect is not None and effect.status == "failed"
            assert effect.reconcile_status == "denied"
            assert worktree is not None
            assert worktree.delivery_state is WorktreeDeliveryState.WORKTREE_RETAINED
        await engine.dispose()

    asyncio.run(scenario())


def test_uncertain_observation_blocks_and_relaunch_is_refused(
    postgres_dsn: str, tmp_path: Path
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        seed = await _seed(factory, tmp_path)
        git = CountingGit()
        registries = _registries(seed.repository)
        # Unreadable/missing target branch => uncertain observation.
        async with factory.begin() as session:
            session.add(
                Effect(
                    schema_version="effect.v1",
                    operation_key=seed.operation_key,
                    step_id=seed.step_id,
                    task_id=seed.task_id,
                    run_id=seed.run_id,
                    effect_class="isolated_mutation",
                    target_kind="git_ref",
                    target_reference="refs/heads/main",
                    idempotency="reconcilable",
                    payload_hash="a" * 64,
                    lease_generation=1,
                    status="dispatched",
                    context={
                        "project_id": "vuzol",
                        "worktree_id": str(seed.worktree_id),
                        "target_branch": "missing-branch",
                        "expected_head": seed.base,
                        "result_commit": seed.result_commit,
                    },
                )
            )
        report = await EffectReconciler(
            factory, git, registries, owner="test-reconciler"
        ).reconcile_startup()
        assert report.uncertain_count == 1
        async with factory() as session:
            effect = await session.scalar(
                select(Effect).where(Effect.operation_key == seed.operation_key)
            )
            step = await session.get(Step, seed.step_id)
            assert effect is not None and effect.status == "uncertain"
            assert step is not None and step.unknown_effects is True

        handler = ResultApplyHandler(factory, registries, git)
        outcome = await handler.execute(_request(seed), CancellationContext())
        assert outcome.kind is OutcomeKind.BLOCKED
        assert git.apply_calls == 0
        await engine.dispose()

    asyncio.run(scenario())


def test_revoked_grant_does_not_start_an_effect(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        seed = await _seed(factory, tmp_path)
        git = CountingGit()
        async with factory.begin() as session:
            approval = await session.get(Approval, seed.approval_id, with_for_update=True)
            assert approval is not None
            approval.status = ApprovalStatus.REJECTED
        handler = ResultApplyHandler(factory, _registries(seed.repository), git)
        outcome = await handler.execute(_request(seed), CancellationContext())
        assert outcome.kind is OutcomeKind.BLOCKED
        assert git.apply_calls == 0
        async with factory() as session:
            effects = list((await session.scalars(select(Effect))).all())
            assert effects == []
        await engine.dispose()

    asyncio.run(scenario())


def test_effect_settle_helper_is_idempotent() -> None:
    effect = Effect(
        operation_key="op",
        step_id=uuid.uuid4(),
        effect_class="isolated_mutation",
        target_kind="git_ref",
        target_reference="refs/heads/main",
        idempotency="reconcilable",
        payload_hash="a" * 64,
        lease_generation=1,
        status="dispatched",
        context={},
    )
    settle_applied(effect, external_ref="refs/heads/main@abc")
    settle_applied(effect, external_ref="refs/heads/main@abc")
    assert effect.status == "settled"
    assert effect.receipt_status == "applied"
