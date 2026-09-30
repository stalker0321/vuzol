"""D2 acceptance/promotion PostgreSQL tests (dossier pp.1,3-11,13 + L1/L3)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.discussion import PlanDraft, PlanItemDraft, WorkPackageService
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.storage.models import (
    AcceptanceEvidence,
    Effect,
    Event,
    MaterializationLink,
    PlanRevision,
    ReviewOutcomeHistory,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import (
    ApprovalStatus,
    IdempotencyClass,
    PlanRevisionCreatedBy,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorkPackageStatus,
    WorktreeDeliveryState,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.acceptance import (
    promotion_gate,
    record_evidence,
    record_waiver,
)

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


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


def _criteria() -> list[dict[str, object]]:
    return [{"criterion_id": "done"}, {"criterion_id": "clean"}]


async def _horizon_package(
    factory: async_sessionmaker[AsyncSession],
    *,
    goal: str | None = "ship the horizon",
    criteria: list[dict[str, object]] | None = None,
    start: bool = True,
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """Create+approve (+start) a pinned horizon package; returns ids."""

    async with UnitOfWork(factory) as uow:
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
            goal=goal,
            exit_criteria=_criteria() if criteria is None and goal else criteria,
        )
        generation = await service.approve(
            package_id=created.package_id,
            revision_number=1,
            h8=created.content_hash[:8],
            expected_status_generation=1,
            user_id=42,
        )
        if start:
            await WorkPackageSequencer(uow).start(
                package_id=created.package_id,
                revision_number=1,
                h8=created.content_hash[:8],
                expected_status_generation=generation,
                user_id=42,
                horizon_enabled=True,
            )
        return created.package_id, created.revision_id, session_id


async def _prove_promotion(
    factory: async_sessionmaker[AsyncSession],
    *,
    package_id: uuid.UUID,
    result_commit: str = "b" * 40,
) -> None:
    """Record a CONSUMED approval envelope for the last item's result (proof)."""

    from vuzol.storage.models import Approval as ApprovalRow
    from vuzol.storage.types import ApprovalStatus as ApprovalStatusRow

    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        links = (
            await uow.session.scalars(
                select(MaterializationLink).where(
                    MaterializationLink.work_package_id == package_id
                )
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
        envelope = {"result_commit": result_commit}
        db_step.payload = {**db_step.payload, "action_envelope": envelope}
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


async def _to_evaluating(factory: async_sessionmaker[AsyncSession], package_id: uuid.UUID) -> None:
    """Drive every materialized item terminal, then exhaust the queue."""

    for _ in range(6):
        async with UnitOfWork(factory) as uow:
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


@pytest.mark.anyio
async def test_d2_goal_path_control_only(postgres_dsn: str) -> None:
    """pp.1: no prod path writes goal silently; SET_GOAL is CAS-fenced."""

    from vuzol.discussion.domain import DomainError

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, _rev, _sess = await _horizon_package(factory, goal=None, criteria=None)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None and package.goal is None
            service = WorkPackageService(uow)
            with pytest.raises(DomainError, match="stale"):
                await service.set_package_goal(
                    package_id=package_id,
                    revision_number=1,
                    h8="ab" * 8,
                    expected_status_generation=999,
                    goal="ship it",
                    exit_criteria=_criteria(),
                    user_id=7,
                )
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_promotion_gate_blocks_without_evidence(postgres_dsn: str) -> None:
    """pp.5 (drill 15): last apply to the real target requires evidence/waiver.

    REDO-2: head moved by intermediate applies (head != base); the gate
    matches the frozen promotion base, never the moving head.
    """

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, _rev, _sess = await _horizon_package(factory)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "c" * 40
            links = (
                await uow.session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == package_id
                    )
                )
            ).all()
            task_id = links[0].task_id
            real_envelope = {
                "target_branch": "main",
                "expected_target_head": "a" * 40,
                "result_commit": "b" * 40,
            }
            with pytest.raises(ValueError, match="final acceptance gate"):
                await promotion_gate(uow.session, task_id=task_id, envelope=real_envelope)
            # intermediate applies (integration branch) pass through untouched
            await promotion_gate(
                uow.session,
                task_id=task_id,
                envelope={
                    "target_branch": "vuzol/package/x",
                    "expected_target_head": "c" * 40,
                    "result_commit": "b" * 40,
                },
            )
            # ...as does a waiver for the promotion base
            await record_waiver(
                uow.session,
                package_id=package_id,
                integration_head="a" * 40,
                principal_user_id=7,
                reason="emergency ship",
            )
            await promotion_gate(uow.session, task_id=task_id, envelope=real_envelope)
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_evidence_unique_per_package_hash(postgres_dsn: str) -> None:
    """pp.13: same content → same row; different content → new row."""

    from vuzol.workflows.acceptance import ACCEPTANCE_EVIDENCE_SCHEMA

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, revision_id, _sess = await _horizon_package(factory)
        doc = {
            "schema": ACCEPTANCE_EVIDENCE_SCHEMA,
            "package_id": str(package_id),
            "plan_revision_id": str(revision_id),
            "plan_content_hash": "ab" * 32,
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
            first = await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document=doc,
                artifact_id=None,
            )
            same = await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document=dict(doc),
                artifact_id=None,
            )
            assert same.id == first.id
            other = await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document={**doc, "result_commit": "c" * 40},
                artifact_id=None,
            )
            assert other.id != first.id
    finally:
        await _engine.dispose()


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
async def test_d2_reject_leaves_corrective_job(postgres_dsn: str) -> None:
    """pp.11: reject acceptance → stays evaluating + durable corrective trace."""

    from vuzol.discussion.domain import DomainError  # noqa: F401

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, revision_id, _sess = await _horizon_package(factory)
        await _to_evaluating(factory, package_id)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            service = WorkPackageService(uow)
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None and package.horizon_phase == "evaluating"
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            await service.record_acceptance(
                package_id=package_id,
                revision_number=1,
                h8=revision.content_hash[:8],
                expected_status_generation=package.version,
                accepted=False,
                artifact_id=None,
                user_id=7,
                horizon_enabled=True,
            )
            assert package.status is WorkPackageStatus.RUNNING
            assert package.horizon_phase == "evaluating"
            trace = await uow.session.scalar(
                select(Event).where(
                    Event.entity_id == package_id,
                    Event.event_type == "work_package.correction_required",
                )
            )
            assert trace is not None
            assert trace.payload["notify"] is True
            outbox = await uow.session.scalar(
                select(func.count())
                .select_from(TransactionalOutbox)
                .where(TransactionalOutbox.linked_entity_id == package_id)
            )
            assert outbox is not None and int(outbox) >= 1
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_set_goal_success_path(postgres_dsn: str) -> None:
    """pp.1 (success): SET_GOAL writes goal/criteria through the control layer."""

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, revision_id, _sess = await _horizon_package(
            factory, goal=None, criteria=None, start=False
        )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            service = WorkPackageService(uow)
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            generation = await service.set_package_goal(
                package_id=package_id,
                revision_number=1,
                h8=revision.content_hash[:8],
                expected_status_generation=package.version,
                goal="ship the horizon",
                exit_criteria=[{"criterion_id": "done"}],
                user_id=7,
            )
            assert generation == package.version
            assert package.goal == "ship the horizon"
            assert package.goal_revision == 1
            assert package.exit_criteria == [{"criterion_id": "done"}]
    finally:
        await _engine.dispose()


def _evidence_doc(
    *,
    package_id: uuid.UUID,
    revision_id: uuid.UUID,
    result_commit: str = "b" * 40,
    criteria: list[dict[str, object]] | None = None,
    plan_hash: str | None = None,
) -> dict[str, object]:
    return {
        "schema": "acceptance-evidence.v1",
        "package_id": str(package_id),
        "plan_revision_id": str(revision_id),
        "plan_content_hash": plan_hash or "ab" * 32,
        "goal": "ship the horizon",
        "goal_revision": 1,
        "spec_revision": None,
        "configuration_revision": "c" * 64,
        "policy_revision": "d" * 64,
        "integration_base_head": "a" * 40,
        "result_commit": result_commit,
        "criteria": (
            criteria
            if criteria is not None
            else [{"criterion_id": "done", "satisfied": True}]
        ),
        "test_results": [],
        "review_refs": ["aa" * 32],
        "unresolved_caveats": [],
        "unresolved_effects": [],
        "created_at": "2026-09-30T00:00:00+00:00",
    }


async def _evidence_artifact(
    factory: async_sessionmaker[AsyncSession],
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    document: dict[str, object],
    root: Path,
) -> uuid.UUID:
    import json as _json

    from vuzol.execution.artifacts import ArtifactStore

    store = ArtifactStore(
        root,
        max_bytes=1_000_000,
        retention_days=7,
        redaction_patterns=(),
    )
    async with UnitOfWork(factory) as uow:
        assert uow.session is not None
        artifact = await store.persist(
            uow.session,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            artifact_type="acceptance_evidence",
            content=_json.dumps(document, sort_keys=True).encode(),
            media_type="application/json",
            sensitivity="internal",
            visibility="private",
        )
        return artifact.id


@pytest.mark.anyio
async def test_d2_evidence_negatives_and_waiver_accept(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """pp.2-4 + waiver: empty/foreign/stale evidence blocked; waiver accepts."""

    from vuzol.discussion.domain import DomainError
    from vuzol.workflows.acceptance import record_evidence

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, revision_id, _sess = await _horizon_package(factory)
        await _to_evaluating(factory, package_id)
        # Phase A (committed): integration refs + a run/step for artifacts.
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "b" * 40
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
            step_id = step_record.id
            foreign_task = await uow.tasks.create(
                user_id=1,
                chat_id=-100,
                original_text="foreign",
                task_type="coding",
                project_id="other-project",
            )
            foreign_task_id = foreign_task.id
        # Phase B: artifacts (own transactions, see committed rows).
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            _rev = await uow.session.get(PlanRevision, revision_id)
            assert _rev is not None
            plan_hash = _rev.content_hash
        empty_doc = _evidence_doc(
            package_id=package_id,
            revision_id=revision_id,
            criteria=[],
            plan_hash=plan_hash,
        )
        empty_artifact = await _evidence_artifact(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            document=empty_doc,
            root=tmp_path,
        )
        foreign_artifact = await _evidence_artifact(
            factory,
            task_id=foreign_task_id,
            run_id=run_id,
            step_id=step_id,
            document=_evidence_doc(package_id=package_id, revision_id=revision_id),
            root=tmp_path,
        )
        stale_doc = _evidence_doc(
            package_id=package_id,
            revision_id=revision_id,
            result_commit="c" * 40,
            plan_hash=plan_hash,
        )
        stale_artifact = await _evidence_artifact(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            document=stale_doc,
            root=tmp_path,
        )
        good_doc = _evidence_doc(
            package_id=package_id, revision_id=revision_id, plan_hash=plan_hash
        )
        good_artifact = await _evidence_artifact(
            factory,
            task_id=task_id,
            run_id=run_id,
            step_id=step_id,
            document=good_doc,
            root=tmp_path,
        )
        # Phase C: negatives (each raises, no state change) + final accept.
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            service = WorkPackageService(uow)
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            h8 = revision.content_hash[:8]
            plan_hash = revision.content_hash

            async def _try_accept(
                *, artifact: uuid.UUID | None, waiver: uuid.UUID | None
            ) -> str:
                try:
                    await service.record_acceptance(
                        package_id=package_id,
                        revision_number=1,
                        h8=h8,
                        expected_status_generation=package.version,
                        accepted=True,
                        artifact_id=artifact,
                        user_id=7,
                        horizon_enabled=True,
                        waiver_id=waiver,
                    )
                except DomainError as error:
                    return str(error)
                return "accepted"

            # empty criteria evidence row (inserted directly: the writer path
            # refuses it, the accept path must refuse it too)
            uow.session.add(
                AcceptanceEvidence(
                    package_id=package_id,
                    plan_revision_id=revision_id,
                    evidence_hash="ee" * 32,
                    integration_base_head="a" * 40,
                    result_commit="b" * 40,
                    artifact_id=empty_artifact,
                    evidence=empty_doc,
                )
            )
            await uow.session.flush()
            assert await _try_accept(artifact=empty_artifact, waiver=None) in (
                "acceptance_criteria_unmet",
                "acceptance_evidence_invalid",
            )
            # foreign artifact (another project) is rejected even with a row
            assert (
                await _try_accept(artifact=foreign_artifact, waiver=None)
                == "acceptance_evidence_foreign"
            )
            # stale head evidence is rejected: no CONSUMED promotion proves
            # that result, so the proof check (not a head pointer compare)
            # fails closed
            await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document=stale_doc,
                artifact_id=stale_artifact,
            )
            assert (
                await _try_accept(artifact=stale_artifact, waiver=None)
                == "acceptance_promotion_unproven"
            )
            # valid evidence accepts (promotion proven via CONSUMED envelope)
            await record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document=good_doc,
                artifact_id=good_artifact,
            )
            await _prove_promotion(factory, package_id=package_id)
            assert await _try_accept(artifact=good_artifact, waiver=None) == "accepted"
            fresh = await uow.session.get(WorkPackage, package_id)
            assert fresh is not None and fresh.status is WorkPackageStatus.COMPLETED
            assert fresh.acceptance_artifact_id == good_artifact
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_waiver_accept_without_artifact(postgres_dsn: str) -> None:
    """L3 waiver: manual accept with principal/reason, no evidence artifact."""

    from vuzol.workflows.acceptance import record_waiver

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, revision_id, _sess = await _horizon_package(factory)
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
            assert package.acceptance_artifact_id is None
    finally:
        await _engine.dispose()


@pytest.mark.anyio
async def test_d2_accept_ingress_completes_package(postgres_dsn: str, tmp_path: Path) -> None:
    """ACCEPT UI: telegram callback → ingress → record_acceptance → COMPLETED."""

    from vuzol.discussion.application import (
        AuthoritativeControlCommand,
        PackageControlIngress,
        PackageControlSource,
    )
    from vuzol.discussion.domain import PackageControlAction
    from vuzol.workflows.acceptance import record_evidence as _record_evidence

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, revision_id, _sess = await _horizon_package(factory)
        await _to_evaluating(factory, package_id)
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "b" * 40
            links = (
                await uow.session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == package_id
                    )
                )
            ).all()
            link_task_id = links[0].task_id
            run_id = await uow.runs.create(
                task_id=link_task_id,
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
            step_id = step_record.id
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            _rev = await uow.session.get(PlanRevision, revision_id)
            assert _rev is not None
            _plan_hash = _rev.content_hash
        doc = _evidence_doc(
            package_id=package_id, revision_id=revision_id, plan_hash=_plan_hash
        )
        artifact_id = await _evidence_artifact(
            factory,
            task_id=link_task_id,
            run_id=run_id,
            step_id=step_id,
            document=doc,
            root=tmp_path,
        )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            await _record_evidence(
                uow.session,
                package_id=package_id,
                plan_revision_id=revision_id,
                document=doc,
                artifact_id=artifact_id,
            )
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None
            revision = await uow.session.get(PlanRevision, revision_id)
            assert revision is not None
            generation = package.version
            content_hash = revision.content_hash
        await _prove_promotion(factory, package_id=package_id)
        ingress = PackageControlIngress(
            factory,
            enabled=True,
            authorized_user_ids=frozenset({7}),
            horizon_enabled=True,
        )
        result = await ingress.apply(
            AuthoritativeControlCommand(
                action=PackageControlAction.ACCEPT_PACKAGE,
                package_id=package_id,
                plan_revision_number=1,
                h8=content_hash[:8],
                expected_status_generation=generation,
                user_id=7,
                source=PackageControlSource.TELEGRAM_CALLBACK,
                external_idempotency_key=f"accept-{uuid.uuid4()}",
            )
        )
        assert result.code.value == "applied"
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id)
            assert package is not None and package.status is WorkPackageStatus.COMPLETED
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
    from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
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
async def test_d2_acceptance_step_assembles_evidence(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """REDO-1/2: pre-apply assembly binds the promotion base; gate opens after."""

    from vuzol.execution.artifacts import ArtifactStore
    from vuzol.storage.models import ReviewOutcomeHistory as HistoryRow
    from vuzol.storage.records import LeaseToken, StepRecord
    from vuzol.workflows.acceptance import AcceptanceGateHandler, promotion_gate
    from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, _revision_id, _sess = await _horizon_package(factory)
        # REDO-1 state: prior items terminal, last item RUNNING but reviewed
        # (acceptance now precedes approve_result, not queue end).
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None
            first_link = await uow.session.scalar(
                select(MaterializationLink).where(
                    MaterializationLink.work_package_id == package_id,
                    MaterializationLink.ordinal == 1,
                )
            )
            assert first_link is not None
            first_task = await uow.session.get(Task, first_link.task_id, with_for_update=True)
            assert first_task is not None
            first_task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(
                task_id=first_link.task_id, horizon_enabled=True
            )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None
            # REDO-2: head moved by intermediate applies, base frozen.
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "c" * 40
            links = (
                await uow.session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == package_id
                    )
                )
            ).all()
            assert len(links) == 2
            first = next(link for link in links if link.ordinal == 1)
            last = next(link for link in links if link.ordinal == 2)
            # D1 review history for the terminal prior item
            prior_run = await uow.runs.create(
                task_id=first.task_id,
                workflow_type="coding",
                workflow_version="4",
                budget_mode="balanced",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
                status=RunStatus.COMPLETED,
            )
            prior_step = await uow.steps.create(
                run_id=prior_run,
                ordinal=1,
                step_type="review",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
            )
            uow.session.add(
                HistoryRow(
                    task_id=first.task_id,
                    run_id=prior_run,
                    step_id=prior_step.id,
                    acceptance_key="aa" * 32,
                    verdict="pass",
                    review_kind="independent",
                    risk="medium",
                    policy_revision="review-policy.v1",
                )
            )
            # last item: RUNNING run, COMPLETED passing review, leased acceptance
            acc_run = await uow.runs.create(
                task_id=last.task_id,
                workflow_type="coding",
                workflow_version="4",
                budget_mode="balanced",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
                status=RunStatus.RUNNING,
            )
            review_step = await uow.steps.create(
                run_id=acc_run,
                ordinal=5,
                step_type="review",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
            )
            db_review = await uow.session.get(Step, review_step.id, with_for_update=True)
            assert db_review is not None
            db_review.status = StepStatus.COMPLETED
            db_review.result = {"verdict": "pass", "summary": "ok"}
            acc_step = await uow.steps.create(
                run_id=acc_run,
                ordinal=9,
                step_type="acceptance",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.LEASED,
            )
            db_step = await uow.session.get(Step, acc_step.id, with_for_update=True)
            assert db_step is not None
            db_step.lease_owner = "owner"
            db_step.lease_generation = 1
            uow.session.add(
                Worktree(
                    task_id=last.task_id,
                    run_id=acc_run,
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
            lease = LeaseToken(
                step=StepRecord(
                    id=db_step.id,
                    run_id=acc_run,
                    status=StepStatus.LEASED,
                    lease_generation=1,
                    lease_owner="owner",
                    lease_expires_at=None,
                ),
                owner="owner",
                generation=1,
            )
            request = StepExecutionRequest(
                task_id=last.task_id,
                run_id=acc_run,
                step_id=db_step.id,
                step_type="acceptance",
                payload={},
                timeout_seconds=120,
                lease=lease,
            )
        store = ArtifactStore(
            tmp_path, max_bytes=5_000_000, retention_days=7, redaction_patterns=()
        )
        handler = AcceptanceGateHandler(factory, artifacts=store)
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.kind.value == "succeeded"
        evidence_id = outcome.result["acceptance_evidence_id"]
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            row = await uow.session.get(AcceptanceEvidence, uuid.UUID(evidence_id))
            assert row is not None
            assert row.artifact_id is not None
            # REDO-2: evidence binds the frozen promotion base, not the
            # moved head, even though head != base after intermediates.
            assert row.integration_base_head == "a" * 40
            assert row.result_commit == "b" * 40
            # the promotion gate opens on the assembled evidence…
            await promotion_gate(
                uow.session,
                task_id=last.task_id,
                envelope={
                    "target_branch": "main",
                    "expected_target_head": "a" * 40,
                    "result_commit": "b" * 40,
                },
            )
            # …but not for a promotion the evidence does not authorize.
            with pytest.raises(ValueError, match="final acceptance gate"):
                await promotion_gate(
                    uow.session,
                    task_id=last.task_id,
                    envelope={
                        "target_branch": "main",
                        "expected_target_head": "c" * 40,
                        "result_commit": "b" * 40,
                    },
                )
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
    from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
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


@pytest.mark.anyio
async def test_d2_e2e_acceptance_then_promotion_apply(
    postgres_dsn: str, tmp_path: Path
) -> None:
    """REDO-1 e2e: acceptance assembles → last-item promotion apply succeeds."""

    from types import SimpleNamespace as _NS
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.config.models import DeliveryMode
    from vuzol.execution.artifacts import ArtifactStore
    from vuzol.execution.result_apply import ResultApplyHandler
    from vuzol.storage.models import Approval as ApprovalRow
    from vuzol.storage.models import ReviewOutcomeHistory as HistoryRow
    from vuzol.storage.records import LeaseToken, StepRecord
    from vuzol.storage.types import ApprovalStatus as ApprovalStatusRow
    from vuzol.workflows.acceptance import AcceptanceGateHandler
    from vuzol.workflows.domain import OutcomeKind
    from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
    from vuzol.workflows.result_approval import envelope_hash

    _engine, factory = storage(postgres_dsn)
    try:
        package_id, _revision_id, _sess = await _horizon_package(factory)
        # item1 terminal, item2 (last) running — pre-apply state
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None
            first_link = await uow.session.scalar(
                select(MaterializationLink).where(
                    MaterializationLink.work_package_id == package_id,
                    MaterializationLink.ordinal == 1,
                )
            )
            assert first_link is not None
            first_task = await uow.session.get(Task, first_link.task_id, with_for_update=True)
            assert first_task is not None
            first_task.status = TaskStatus.COMPLETED
            await WorkPackageSequencer(uow).observe_terminal(
                task_id=first_link.task_id, horizon_enabled=True
            )
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            package = await uow.session.get(WorkPackage, package_id, with_for_update=True)
            assert package is not None
            package.integration_branch = "vuzol/package/x"
            package.integration_target_branch = "main"
            package.integration_base_commit = "a" * 40
            package.integration_head_commit = "c" * 40
            links = (
                await uow.session.scalars(
                    select(MaterializationLink).where(
                        MaterializationLink.work_package_id == package_id
                    )
                )
            ).all()
            first = next(link for link in links if link.ordinal == 1)
            last = next(link for link in links if link.ordinal == 2)
            prior_run = await uow.runs.create(
                task_id=first.task_id,
                workflow_type="coding",
                workflow_version="4",
                budget_mode="balanced",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
                status=RunStatus.COMPLETED,
            )
            prior_step = await uow.steps.create(
                run_id=prior_run,
                ordinal=1,
                step_type="review",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
            )
            uow.session.add(
                HistoryRow(
                    task_id=first.task_id,
                    run_id=prior_run,
                    step_id=prior_step.id,
                    acceptance_key="aa" * 32,
                    verdict="pass",
                    review_kind="independent",
                    risk="medium",
                    policy_revision="review-policy.v1",
                )
            )
            acc_run = await uow.runs.create(
                task_id=last.task_id,
                workflow_type="coding",
                workflow_version="4",
                budget_mode="balanced",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
                status=RunStatus.RUNNING,
            )
            review_step = await uow.steps.create(
                run_id=acc_run,
                ordinal=5,
                step_type="review",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
            )
            db_review = await uow.session.get(Step, review_step.id, with_for_update=True)
            assert db_review is not None
            db_review.status = StepStatus.COMPLETED
            db_review.result = {"verdict": "pass", "summary": "ok"}
            acc_step = await uow.steps.create(
                run_id=acc_run,
                ordinal=9,
                step_type="acceptance",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.LEASED,
            )
            db_step = await uow.session.get(Step, acc_step.id, with_for_update=True)
            assert db_step is not None
            db_step.lease_owner = "owner"
            db_step.lease_generation = 1
            uow.session.add(
                Worktree(
                    task_id=last.task_id,
                    run_id=acc_run,
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
            # approval step for the same run, leased, with a real-target envelope
            approval_id = uuid.uuid4()
            envelope: dict[str, object] = {
                "schema_version": "result-approval.v1",
                "requested_action": "apply_result",
                "task_id": str(last.task_id),
                "run_id": str(acc_run),
                "configuration_revision": "c" * 64,
                "policy_revision": "d" * 64,
                "project_id": "vuzol",
                "repository_identity_hash": "r" * 64,
                "target_branch": "main",
                "expected_target_head": "a" * 40,
                "base_commit": "a" * 40,
                "result_commit": "b" * 40,
                "diff_hash": "c" * 64,
            }
            approval_step = await uow.steps.create(
                run_id=acc_run,
                ordinal=10,
                step_type="approval",
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                status=StepStatus.LEASED,
            )
            db_approval_step = await uow.session.get(
                Step, approval_step.id, with_for_update=True
            )
            assert db_approval_step is not None
            db_approval_step.lease_owner = "owner"
            db_approval_step.lease_generation = 1
            envelope["step_id"] = str(db_approval_step.id)
            db_approval_step.payload = {
                "approval_id": str(approval_id),
                "action_envelope": envelope,
            }
            uow.session.add(
                ApprovalRow(
                    id=approval_id,
                    step_id=db_approval_step.id,
                    action_envelope_hash=envelope_hash(envelope),
                    requested_action="apply_result",
                    normalized_target="vuzol:main",
                    human_summary="apply",
                    token_hash=f"ff{uuid.uuid4().hex[:56]}",
                    status=ApprovalStatusRow.APPROVED,
                    expires_at=datetime.now(UTC) + timedelta(days=1),
                )
            )
            acc_lease = LeaseToken(
                step=StepRecord(
                    id=db_step.id,
                    run_id=acc_run,
                    status=StepStatus.LEASED,
                    lease_generation=1,
                    lease_owner="owner",
                    lease_expires_at=None,
                ),
                owner="owner",
                generation=1,
            )
            acc_request = StepExecutionRequest(
                task_id=last.task_id,
                run_id=acc_run,
                step_id=db_step.id,
                step_type="acceptance",
                payload={},
                timeout_seconds=120,
                lease=acc_lease,
            )
            apply_lease = LeaseToken(
                step=StepRecord(
                    id=db_approval_step.id,
                    run_id=acc_run,
                    status=StepStatus.LEASED,
                    lease_generation=1,
                    lease_owner="owner",
                    lease_expires_at=None,
                ),
                owner="owner",
                generation=1,
            )
            apply_request = StepExecutionRequest(
                task_id=last.task_id,
                run_id=acc_run,
                step_id=db_approval_step.id,
                step_type="approval",
                payload={},
                timeout_seconds=120,
                lease=apply_lease,
            )
        # 1) acceptance assembles evidence (would deadlock pre-REDO)
        store = ArtifactStore(
            tmp_path, max_bytes=5_000_000, retention_days=7, redaction_patterns=()
        )
        gate_outcome = await AcceptanceGateHandler(factory, artifacts=store).execute(
            acc_request, CancellationContext()
        )
        assert gate_outcome.kind.value == "succeeded"
        # 2) the promotion apply finds the gate open and settles
        git = MagicMock()
        git.repository_identity = AsyncMock(return_value=("r" * 64, None))
        git.apply_result = AsyncMock(return_value=True)
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
        apply_outcome = await ResultApplyHandler(factory, registries, git).execute(
            apply_request, CancellationContext()
        )
        assert apply_outcome.kind is OutcomeKind.SUCCEEDED
        async with UnitOfWork(factory) as uow:
            assert uow.session is not None
            approval = await uow.session.get(ApprovalRow, approval_id)
            assert approval is not None and approval.status is ApprovalStatus.CONSUMED
            from vuzol.storage.models import Effect as EffectRow

            effect = await uow.session.scalar(
                select(EffectRow).where(EffectRow.step_id == db_approval_step.id)
            )
            assert effect is not None and effect.status == "settled"
    finally:
        await _engine.dispose()
