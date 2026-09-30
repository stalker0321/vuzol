"""D1 identity/revisions PostgreSQL tests (dossier pp.3,4,7,8 + L1/L2/L6)."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.storage.attempts import (
    close_work_attempt,
    latest_attempt,
    record_review_outcome,
    record_work_attempt,
    resolve_stable_item,
    snapshot_task_spec,
)
from vuzol.storage.models import (
    Approval,
    ReviewOutcomeHistory,
    Step,
    Task,
    TaskSpecRevision,
    WorkAttempt,
)
from vuzol.storage.records import LeaseToken, StepRecord, TaskRecord
from vuzol.storage.types import (
    ApprovalStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.controls import decide_result
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.service import commit_step_outcome

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _verdict() -> dict[str, object]:
    return {
        "verdict": "blocked",
        "review_kind": "independent",
        "risk": "high",
        "base_commit": "a" * 40,
        "result_commit": "b" * 40,
        "diff_hash": "c" * 64,
        "changed_files": ["src/app.py"],
        "findings": [
            {
                "severity": "blocker",
                "classification": "shell_execution",
                "summary": "shell in diff",
                "path": "src/app.py",
                "line": 3,
            }
        ],
        "summary": "Independent review blocked: shell_execution.",
        "policy_revision": "review-policy.v1",
        "partition_count": 1,
        "unknown_usage": False,
    }


async def _leased_step(
    factory: object, *, step_status: StepStatus = StepStatus.RUNNING
) -> tuple[TaskRecord, uuid.UUID, Step, LeaseToken]:
    """Seed task/run/step and fence a lease on the step; returns ids + token."""


    task_record, run_id, step_record = await seed_task_run_step(
        factory,  # type: ignore[arg-type]
        step_status=StepStatus.QUEUED,
        step_type="review",
    )
    async with UnitOfWork(factory) as uow:  # type: ignore[arg-type]
        assert uow.session is not None
        step = await uow.session.get(Step, step_record.id, with_for_update=True)
        assert step is not None
        step.status = step_status
        step.lease_owner = "owner"
        step.lease_generation = 1
        token = LeaseToken(
            step=StepRecord(
                id=step.id,
                run_id=run_id,
                status=step_status,
                lease_generation=1,
                lease_owner="owner",
                lease_expires_at=None,
            ),
            owner="owner",
            generation=1,
        )
        return task_record, run_id, step, token


@pytest.mark.anyio
async def test_d1_blocked_verdict_reaches_history(postgres_dsn: str) -> None:
    """pp.3: BLOCKED review verdict persists findings/diff/policy to history."""

    _engine, factory = storage(postgres_dsn)
    try:
        task_record, run_id, step, token = await _leased_step(factory)
        outcome = StepOutcome(
            kind=OutcomeKind.BLOCKED,
            result=_verdict(),
            category="review_blocked",
            summary="blocked",
            unknown_effects=False,
        )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            await commit_step_outcome(uow.session, token, outcome)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            row = await uow.session.scalar(
                select(ReviewOutcomeHistory).where(ReviewOutcomeHistory.step_id == step.id)
            )
            assert row is not None
            assert row.verdict == "blocked"
            assert row.diff_hash == "c" * 64
            assert row.policy_revision == "review-policy.v1"
            assert row.findings[0]["classification"] == "shell_execution"
            # Step.result itself still does not carry the verdict (by design)
            fresh = await uow.session.get(Step, step.id)
            assert fresh is not None
            assert fresh.result is None or "verdict" not in (fresh.result or {})
            # REDO condition 1: a second BLOCKED with different content is
            # recorded, never swallowed by the first row's key
            other = dict(_verdict())
            other["summary"] = "Independent review blocked: another defect."
            second = await record_review_outcome(
                uow.session,
                task_id=task_record.id,
                run_id=run_id,
                step_id=step.id,
                verdict=other,
            )
            assert second.id != row.id
            # ...while an identical re-commit is idempotent (safe retries)
            same = await record_review_outcome(
                uow.session,
                task_id=task_record.id,
                run_id=run_id,
                step_id=step.id,
                verdict=_verdict(),
            )
            assert same.id == row.id
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d1_repeat_redo_rejects_and_keeps_candidate(postgres_dsn: str) -> None:
    """pp.4 + L6: REDO records an attempt with prior refs; 2nd REDO rejected."""

    _engine, factory = storage(postgres_dsn)
    try:
        task_record, _run_id, step, _token = await _leased_step(
            factory, step_status=StepStatus.WAITING_APPROVAL
        )
        candidate = {"verdict": "pass", "summary": "looks good", "result_commit": "b" * 40}
        approval_id = uuid.uuid4()
        from vuzol.workflows.result_approval import envelope_hash

        envelope = {"schema_version": "result-approval.v1", "step_id": str(step.id)}
        digest = envelope_hash(envelope)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            db_step = await uow.session.get(Step, step.id, with_for_update=True)
            assert db_step is not None
            db_step.result = dict(candidate)
            db_step.payload = {**db_step.payload, "action_envelope": envelope}
            uow.session.add(
                Approval(
                    id=approval_id,
                    step_id=step.id,
                    action_envelope_hash=digest,
                    requested_action="apply_result",
                    normalized_target="proj:main",
                    human_summary="apply",
                    token_hash="e" * 64,
                    status=ApprovalStatus.PENDING,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            await decide_result(
                uow.session, approval_id, decision="redo", deciding_user_id=7
            )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            attempts = (
                await uow.session.scalars(
                    select(WorkAttempt).where(WorkAttempt.step_id == step.id)
                )
            ).all()
            assert len(attempts) == 1
            attempt = attempts[0]
            assert attempt.outcome == "cancelled"
            assert attempt.attempt_kind == "retry"
            assert attempt.parent_attempt_id is None
            assert attempt.stable_item_id == task_record.id
            assert attempt.prior_candidate_hash == hashlib.sha256(
                json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            assert attempt.prior_review_summary == "looks good"
            # candidate/review survive on the step
            kept = await uow.session.get(Step, step.id)
            assert kept is not None and kept.result == candidate
            with pytest.raises(ValueError, match="already decided"):
                await decide_result(
                    uow.session, approval_id, decision="redo", deciding_user_id=7
                )
            kept2 = await uow.session.get(Step, step.id)
            assert kept2 is not None and kept2.result == candidate
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d1_attempt_uniqueness_lineage_close(postgres_dsn: str) -> None:
    """pp.7: attempt_no unique within step, parent lineage, close is append-only."""

    _engine, factory = storage(postgres_dsn)
    try:
        task_record, run_id, step, _token = await _leased_step(factory)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            first = await record_work_attempt(
                uow.session,
                task_id=task_record.id,
                run_id=run_id,
                step_id=step.id,
                attempt_kind="initial",
                purpose="coding",
                stable_item_id=task_record.id,
            )
            assert first.attempt_no == 1
            second = await record_work_attempt(
                uow.session,
                task_id=task_record.id,
                run_id=run_id,
                step_id=step.id,
                attempt_kind="retry",
                purpose="coding",
                parent_attempt_id=first.id,
                stable_item_id=task_record.id,
            )
            assert second.attempt_no == 2
            assert second.parent_attempt_id == first.id
            assert await latest_attempt(uow.session, step.id) is not None
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            # same attempt_no twice → unique violation (never silent reorder)
            clash = WorkAttempt(
                task_id=task_record.id,
                run_id=run_id,
                step_id=step.id,
                attempt_no=2,
                attempt_kind="retry",
                purpose="coding",
                lease_generation=0,
                outcome="running",
            )
            uow.session.add(clash)
            with pytest.raises(IntegrityError):
                await uow.session.flush()
            # the failed flush poisons the unit of work; roll back the clash
            # so the context exit can commit cleanly
            await uow.session.rollback()
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            # close is final: a second close raises instead of mutating
            reopened = await latest_attempt(uow.session, step.id)
            assert reopened is not None
            await close_work_attempt(uow.session, reopened, outcome="failed")
            with pytest.raises(ValueError, match="already closed"):
                await close_work_attempt(uow.session, reopened, outcome="failed")
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d1_spec_versions_and_legacy_provenance(postgres_dsn: str) -> None:
    """pp.8 + L2: spec snapshots version the draft; legacy rows stay NULL."""

    _engine, factory = storage(postgres_dsn)
    try:
        task_record, _run_id, _step, _token = await _leased_step(factory)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            task = await uow.session.get(Task, task_record.id, with_for_update=True)
            assert task is not None
            # creation snapshot exists (via TaskRepository.create)
            assert task.spec_revision is not None
            first = task.spec_revision
            rows = (
                await uow.session.scalars(
                    select(TaskSpecRevision).where(TaskSpecRevision.task_id == task.id)
                )
            ).all()
            assert len(rows) == 1
            # unchanged draft reuses the row
            assert await snapshot_task_spec(uow.session, task) == first
            rows = (
                await uow.session.scalars(
                    select(TaskSpecRevision).where(TaskSpecRevision.task_id == task.id)
                )
            ).all()
            assert len(rows) == 1
            # mutated draft gets a new row; pointer moves; history kept
            task.task_draft = {**task.task_draft, "goal": "a new goal"}
            second = await snapshot_task_spec(uow.session, task)
            assert second != first and task.spec_revision == second
            rows = (
                await uow.session.scalars(
                    select(TaskSpecRevision).where(TaskSpecRevision.task_id == task.id)
                )
            ).all()
            assert len(rows) == 2
            # legacy provenance: NULL source/spec on a directly inserted row
            legacy = Task(
                user_id=1,
                original_text="legacy",
                task_draft={},
                task_type="coding",
                status=TaskStatus.RECEIVED,
            )
            uow.session.add(legacy)
            await uow.session.flush()
            assert legacy.source_turn_id is None and legacy.spec_revision is None
            # stable item falls back to the task itself outside packages
            assert await resolve_stable_item(uow.session, legacy.id) == legacy.id
    finally:
        await _engine.dispose()
