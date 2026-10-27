"""Apply, finalize and reconciliation PostgreSQL tests (D2 dossier)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.storage.models import (
    Effect,
    ReviewOutcomeHistory,
    Run,
    Step,
    Worktree,
)
from vuzol.storage.types import (
    ApprovalStatus,
    RunStatus,
    StepStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.ports import CancellationContext

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.mark.anyio
async def test_d2_finalize_holds_on_active_effect(postgres_dsn: str) -> None:
    """pp.10: Task/Run COMPLETED impossible while an effect is unsettled."""

    from vuzol.workflows.service import finalize_if_complete

    _engine, factory = storage(postgres_dsn)
    try:
        _task, run_id, step_record = await seed_task_run_step(
            factory,
            step_status=StepStatus.COMPLETED,
        )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            run = await uow.session.get(Run, run_id, with_for_update=True)
            assert run is not None
            run.status = RunStatus.RUNNING
            uow.session.add(
                Effect(
                    operation_key=f"op-{uuid.uuid4()}",
                    step_id=step_record.id,
                    effect_class="isolated_mutation",
                    target_kind="git_ref",
                    target_reference="refs/heads/main",
                    idempotency="reconcilable",
                    payload_hash="a" * 64,
                    lease_generation=1,
                    status="dispatched",
                    context={},
                )
            )
            assert await finalize_if_complete(uow.session, run) is False
            assert run.status is RunStatus.RUNNING
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_reconciler_settles_without_reapply(postgres_dsn: str) -> None:
    """pp.7 (drill 4): applied + lost receipt → settled, apply never reruns."""

    from vuzol.execution.effect_reconciliation import EffectReconciler

    _engine, factory = storage(postgres_dsn)
    try:
        _task, run_id, step_record = await seed_task_run_step(
            factory,
            step_status=StepStatus.COMPLETED,
        )
        result_commit = "b" * 40
        applied_calls: list[tuple[object, ...]] = []

        class _Git:
            async def read_ref(self, repository: object, branch: str) -> str | None:
                assert branch == "main"
                return result_commit

            async def apply_result(self, *args: object) -> bool:
                applied_calls.append(args)
                return True

        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            worktree = Worktree(
                task_id=_task.id,
                run_id=run_id,
                project_id="vuzol",
                repository_identity_hash="r" * 64,
                base_commit="a" * 40,
                default_branch="main",
                expected_target_head="a" * 40,
                branch="wt-1",
                path=f"memory-wt-{uuid.uuid4()}",
                owner="test",
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                result_commit=result_commit,
                retention_until=datetime.now(UTC) + timedelta(days=1),
            )
            uow.session.add(worktree)
            await uow.session.flush()
            uow.session.add(
                Effect(
                    operation_key=f"op-{uuid.uuid4()}",
                    step_id=step_record.id,
                    effect_class="isolated_mutation",
                    target_kind="git_ref",
                    target_reference="refs/heads/main",
                    idempotency="reconcilable",
                    payload_hash="a" * 64,
                    lease_generation=1,
                    status="dispatched",
                    context={
                        "target_branch": "main",
                        "result_commit": result_commit,
                        "expected_head": "a" * 40,
                        "repository_path": "memory-repo",
                        "worktree_id": str(worktree.id),
                    },
                )
            )
        report = await EffectReconciler(
            factory,
            _Git(),  # type: ignore[arg-type]
            MagicMock(),
            owner="test:reconcile",
        ).reconcile_startup()
        assert report.lock_acquired is True
        assert applied_calls == []
        assert report.confirmed_count == 1
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            effect = await uow.session.scalar(
                select(Effect).where(Effect.step_id == step_record.id)
            )
            assert effect is not None and effect.status == "settled"
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_repeated_blocked_appends_history(postgres_dsn: str) -> None:
    """pp.6 (drill 3): repeated BLOCKED commits accumulate verdicts, no loss."""

    from vuzol.storage.records import LeaseToken, StepRecord
    from vuzol.workflows.domain import OutcomeKind, StepOutcome
    from vuzol.workflows.service import commit_step_outcome

    _engine, factory = storage(postgres_dsn)
    try:
        _task_record, run_id, step_record = await seed_task_run_step(
            factory,
            step_status=StepStatus.RUNNING,
            step_type="review",
        )

        async def _commit(verdict_summary: str, generation: int) -> None:
            async with UnitOfWork(factory) as uow:
                assert uow.session is not None
                step = await uow.session.get(Step, step_record.id, with_for_update=True)
                assert step is not None
                step.status = StepStatus.RUNNING
                step.lease_owner = "owner"
                step.lease_generation = generation
                run = await uow.session.get(Run, run_id, with_for_update=True)
                assert run is not None
                run.status = RunStatus.RUNNING
                token = LeaseToken(
                    step=StepRecord(
                        id=step.id,
                        run_id=run_id,
                        status=StepStatus.RUNNING,
                        lease_generation=generation,
                        lease_owner="owner",
                        lease_expires_at=None,
                    ),
                    owner="owner",
                    generation=generation,
                )
                verdict = dict(_d2_blocked_verdict())
                verdict["summary"] = verdict_summary
                await commit_step_outcome(
                    uow.session,
                    token,
                    StepOutcome(
                        kind=OutcomeKind.BLOCKED,
                        result=verdict,
                        category="review_blocked",
                        summary=verdict_summary,
                        unknown_effects=False,
                    ),
                )

        await _commit("first block", 1)
        await _commit("second block", 2)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            rows = (
                await uow.session.scalars(
                    select(ReviewOutcomeHistory).where(
                        ReviewOutcomeHistory.step_id == step_record.id
                    )
                )
            ).all()
            assert len(rows) == 2
            assert {row.summary for row in rows} == {"first block", "second block"}
    finally:
        await _engine.dispose()


def _d2_blocked_verdict() -> dict[str, object]:
    return {
        "verdict": "blocked",
        "review_kind": "independent",
        "risk": "high",
        "base_commit": "a" * 40,
        "result_commit": "b" * 40,
        "diff_hash": "c" * 64,
        "changed_files": ["src/app.py"],
        "findings": [],
        "summary": "blocked",
        "policy_revision": "review-policy.v1",
        "partition_count": 1,
        "unknown_usage": False,
    }


@pytest.mark.anyio
async def test_d2_apply_head_drift_blocked_not_failed(postgres_dsn: str) -> None:
    """pp.8 (drill 10): target drift → BLOCKED approved_result_not_applied."""

    from vuzol.execution.git import GitError
    from vuzol.execution.result_apply import ResultApplyHandler
    from vuzol.storage.models import Approval as ApprovalRow
    from vuzol.storage.records import LeaseToken, StepRecord
    from vuzol.workflows.domain import OutcomeKind
    from vuzol.workflows.ports import StepExecutionRequest
    from vuzol.workflows.result_approval import envelope_hash

    _engine, factory = storage(postgres_dsn)
    try:
        task_record, run_id, step_record = await seed_task_run_step(
            factory,
            step_status=StepStatus.RUNNING,
            step_type="approval",
        )
        approval_id = uuid.uuid4()
        envelope: dict[str, object] = {
            "schema_version": "result-approval.v1",
            "requested_action": "apply_result",
            "task_id": str(task_record.id),
            "run_id": str(run_id),
            "step_id": str(step_record.id),
            "project_id": "vuzol",
            "repository_identity_hash": "r" * 64,
            "target_branch": "main",
            "expected_target_head": "a" * 40,
            "base_commit": "a" * 40,
            "result_commit": "b" * 40,
            "diff_hash": "c" * 64,
            "agent_checks": [],
            "gates": [],
            "changed_files": [],
            "validation_evidence_hash": "d" * 64,
            "review_evidence": None,
            "review_evidence_hash": None,
            "static_build_evidence": None,
            "artifact_evidence": None,
            "configuration_revision": "c" * 64,
            "policy_revision": "d" * 64,
        }
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            step = await uow.session.get(Step, step_record.id, with_for_update=True)
            assert step is not None
            step.status = StepStatus.RUNNING
            step.lease_owner = "owner"
            step.lease_generation = 1
            step.payload = {
                "approval_id": str(approval_id),
                "action_envelope": envelope,
            }
            run = await uow.session.get(Run, run_id, with_for_update=True)
            assert run is not None
            run.configuration_revision = "c" * 64
            run.policy_revision = "d" * 64
            uow.session.add(
                ApprovalRow(
                    id=approval_id,
                    step_id=step_record.id,
                    action_envelope_hash=envelope_hash(envelope),
                    requested_action="apply_result",
                    normalized_target="vuzol:main",
                    human_summary="apply",
                    token_hash="e" * 64,
                    status=ApprovalStatus.APPROVED,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
            uow.session.add(
                Worktree(
                    task_id=task_record.id,
                    run_id=run_id,
                    project_id="vuzol",
                    repository_identity_hash="r" * 64,
                    base_commit="a" * 40,
                    default_branch="main",
                    expected_target_head="a" * 40,
                    branch="wt-1",
                    path=f"memory-wt-{uuid.uuid4()}",
                    owner="test",
                    delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                    result_commit="b" * 40,
                    diff_hash="c" * 64,
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        git = MagicMock()
        git.repository_identity = AsyncMock(return_value=("r" * 64, None))
        git.apply_result = AsyncMock(
            side_effect=GitError("target branch changed after the result was produced")
        )
        git.require_apply_access = AsyncMock()
        from types import SimpleNamespace as _NS

        from vuzol.config.models import DeliveryMode

        project = _NS(
            enabled=True,
            default_branch="main",
            repository_path="memory-repo",
            git_delivery=_NS(
                allowed_modes={DeliveryMode.APPLY},
                approval_required={DeliveryMode.APPLY},
            ),
        )
        registries = MagicMock()
        registries.projects.get = MagicMock(return_value=project)
        handler = ResultApplyHandler(factory, registries, git)
        request = StepExecutionRequest(
            task_id=task_record.id,
            run_id=run_id,
            step_id=step_record.id,
            step_type="approval",
            payload={},
            timeout_seconds=120,
            lease=LeaseToken(
                step=StepRecord(
                    id=step_record.id,
                    run_id=run_id,
                    status=StepStatus.RUNNING,
                    lease_generation=1,
                    lease_owner="owner",
                    lease_expires_at=None,
                ),
                owner="owner",
                generation=1,
            ),
        )
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.kind is OutcomeKind.BLOCKED
        assert outcome.category == "approved_result_not_applied"
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_apply_revision_drift_blocked(postgres_dsn: str) -> None:
    """pp.9 (L4 barrier, apply side): envelope revisions drifted → BLOCKED."""

    from unittest.mock import MagicMock

    from vuzol.execution.result_apply import ResultApplyHandler
    from vuzol.storage.models import Approval as ApprovalRow
    from vuzol.storage.records import LeaseToken, StepRecord
    from vuzol.workflows.domain import OutcomeKind
    from vuzol.workflows.ports import StepExecutionRequest
    from vuzol.workflows.result_approval import envelope_hash

    _engine, factory = storage(postgres_dsn)
    try:
        task_record, run_id, step_record = await seed_task_run_step(
            factory,
            step_status=StepStatus.RUNNING,
            step_type="approval",
        )
        approval_id = uuid.uuid4()
        envelope: dict[str, object] = {
            "schema_version": "result-approval.v1",
            "requested_action": "apply_result",
            "task_id": str(task_record.id),
            "run_id": str(run_id),
            "step_id": str(step_record.id),
            "project_id": "vuzol",
            "repository_identity_hash": "r" * 64,
            "target_branch": "main",
            "expected_target_head": "a" * 40,
            "base_commit": "a" * 40,
            "result_commit": "b" * 40,
            "diff_hash": "c" * 64,
            "agent_checks": [],
            "gates": [],
            "changed_files": [],
            "validation_evidence_hash": "d" * 64,
            "review_evidence": None,
            "review_evidence_hash": None,
            "static_build_evidence": None,
            "artifact_evidence": None,
            "configuration_revision": "c" * 64,
            "policy_revision": "d" * 64,
        }
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            step = await uow.session.get(Step, step_record.id, with_for_update=True)
            assert step is not None
            step.status = StepStatus.RUNNING
            step.lease_owner = "owner"
            step.lease_generation = 1
            step.payload = {
                "approval_id": str(approval_id),
                "action_envelope": envelope,
            }
            run = await uow.session.get(Run, run_id, with_for_update=True)
            assert run is not None
            # configuration drifted after the envelope was requested
            run.configuration_revision = "c" * 63 + "X"
            run.policy_revision = "d" * 64
            uow.session.add(
                ApprovalRow(
                    id=approval_id,
                    step_id=step_record.id,
                    action_envelope_hash=envelope_hash(envelope),
                    requested_action="apply_result",
                    normalized_target="vuzol:main",
                    human_summary="apply",
                    token_hash="e" * 64,
                    status=ApprovalStatus.APPROVED,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
            uow.session.add(
                Worktree(
                    task_id=task_record.id,
                    run_id=run_id,
                    project_id="vuzol",
                    repository_identity_hash="r" * 64,
                    base_commit="a" * 40,
                    default_branch="main",
                    expected_target_head="a" * 40,
                    branch="wt-1",
                    path=f"memory-wt-{uuid.uuid4()}",
                    owner="test",
                    delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                    result_commit="b" * 40,
                    diff_hash="c" * 64,
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        handler = ResultApplyHandler(factory, MagicMock(), MagicMock())
        request = StepExecutionRequest(
            task_id=task_record.id,
            run_id=run_id,
            step_id=step_record.id,
            step_type="approval",
            payload={},
            timeout_seconds=120,
            lease=LeaseToken(
                step=StepRecord(
                    id=step_record.id,
                    run_id=run_id,
                    status=StepStatus.RUNNING,
                    lease_generation=1,
                    lease_owner="owner",
                    lease_expires_at=None,
                ),
                owner="owner",
                generation=1,
            ),
        )
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.kind is OutcomeKind.BLOCKED
        assert outcome.category == "approved_result_not_applied"
        assert "drifted" in (outcome.summary or "")
    finally:
        await _engine.dispose()


_KEEPS_STATEMENT = "saved work remains after process restart and database reconnect"
_KEEPS_CRITERIA = [
    {"criterion_id": "keeps-saved-work", "statement": _KEEPS_STATEMENT},
]
