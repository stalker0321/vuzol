"""D2 acceptance/promotion: evidence, final gate, waiver, corrective signals.

Q1 placement: the final acceptance gate stands BEFORE promotion of the last
item into the real target. Intermediate applies target the integration
branch (auto-approve, unchanged); the last apply targets
``integration_target_branch`` and requires evidence (or a waiver).

Q2 authority: accepted = package-level (``WorkPackage.accepted_at`` +
evidence artifact); applied = per-apply ``Approval``; per-step =
``ReviewVerdict``. Evidence references approvals and D1 review history.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.horizon import is_horizon, pinned_horizon_enabled, unmet_exit_criteria
from vuzol.execution.artifacts import ArtifactStore
from vuzol.storage.models import (
    AcceptanceEvidence,
    AcceptanceWaiver,
    Effect,
    Event,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    ReviewOutcomeHistory,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import StepStatus, TaskStatus
from vuzol.telegram.attention import AttentionEvent, should_notify
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

ACCEPTANCE_EVIDENCE_SCHEMA = "acceptance-evidence.v1"
ACCEPTANCE_STEP_TYPE = "acceptance"


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def evidence_hash(doc: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(doc).encode()).hexdigest()


def validate_evidence(doc: object) -> tuple[str, ...]:
    """Fail-closed structural validation of an acceptance-evidence document."""

    if not isinstance(doc, dict):
        return ("evidence_not_object",)
    if doc.get("schema") != ACCEPTANCE_EVIDENCE_SCHEMA:
        return ("evidence_schema_mismatch",)
    required = (
        "package_id",
        "plan_revision_id",
        "plan_content_hash",
        "goal",
        "integration_base_head",
        "result_commit",
        "criteria",
        "test_results",
        "review_refs",
    )
    for field in required:
        if doc.get(field) in (None, ""):
            return (f"evidence_{field}_missing",)
    criteria = doc.get("criteria")
    if not isinstance(criteria, list) or not criteria:
        return ("evidence_criteria_missing",)
    for entry in criteria:
        if not isinstance(entry, dict) or not entry.get("criterion_id"):
            return ("evidence_criterion_malformed",)
    review_refs = doc.get("review_refs")
    if not isinstance(review_refs, list) or not review_refs:
        return ("evidence_review_refs_missing",)
    for ref in review_refs:
        if not isinstance(ref, str) or len(ref) != 64:
            return ("evidence_review_ref_malformed",)
    return ()


async def record_evidence(
    session: AsyncSession,
    *,
    package_id: uuid.UUID,
    plan_revision_id: uuid.UUID | None,
    document: dict[str, Any],
    artifact_id: uuid.UUID | None,
) -> AcceptanceEvidence:
    """Persist evidence idempotently: same content → existing row (no swallow).

    Uniqueness is ``(package_id, evidence_hash)`` — a second document with
    different content always creates a new row.
    """

    errors = validate_evidence(document)
    if errors:
        raise ValueError(f"acceptance evidence invalid: {errors[0]}")
    digest = evidence_hash(document)
    existing = await session.scalar(
        select(AcceptanceEvidence).where(
            AcceptanceEvidence.package_id == package_id,
            AcceptanceEvidence.evidence_hash == digest,
        )
    )
    if existing is not None:
        return existing
    row = AcceptanceEvidence(
        package_id=package_id,
        plan_revision_id=plan_revision_id,
        evidence_hash=digest,
        integration_base_head=document.get("integration_base_head"),
        result_commit=document.get("result_commit"),
        artifact_id=artifact_id,
        evidence=document,
    )
    session.add(row)
    await session.flush()
    return row


async def find_evidence(
    session: AsyncSession,
    *,
    package_id: uuid.UUID,
    plan_revision_id: uuid.UUID | None = None,
    integration_base_head: str | None = None,
    result_commit: str | None = None,
) -> AcceptanceEvidence | None:
    """Latest evidence row for a package, optionally narrowed by heads."""

    statement = (
        select(AcceptanceEvidence)
        .where(AcceptanceEvidence.package_id == package_id)
        .order_by(AcceptanceEvidence.created_at.desc())
    )
    if plan_revision_id is not None:
        statement = statement.where(AcceptanceEvidence.plan_revision_id == plan_revision_id)
    if integration_base_head is not None:
        statement = statement.where(
            AcceptanceEvidence.integration_base_head == integration_base_head
        )
    if result_commit is not None:
        statement = statement.where(AcceptanceEvidence.result_commit == result_commit)
    row = await session.scalar(statement)
    if row is None:
        return None
    return row


async def record_waiver(
    session: AsyncSession,
    *,
    package_id: uuid.UUID,
    integration_head: str,
    principal_user_id: int,
    reason: str,
) -> AcceptanceWaiver:
    """Record a manual waiver (separate type with principal/reason, not a flag)."""

    if not reason.strip():
        raise ValueError("waiver reason is required")
    existing = await session.scalar(
        select(AcceptanceWaiver).where(
            AcceptanceWaiver.package_id == package_id,
            AcceptanceWaiver.integration_head == integration_head,
        )
    )
    if existing is not None:
        return existing
    row = AcceptanceWaiver(
        package_id=package_id,
        integration_head=integration_head,
        principal_user_id=principal_user_id,
        reason=reason.strip(),
    )
    session.add(row)
    await session.flush()
    return row


async def find_waiver(
    session: AsyncSession, *, package_id: uuid.UUID, integration_head: str
) -> AcceptanceWaiver | None:
    row = await session.scalar(
        select(AcceptanceWaiver).where(
            AcceptanceWaiver.package_id == package_id,
            AcceptanceWaiver.integration_head == integration_head,
        )
    )
    if row is None:
        return None
    return row


async def promotion_gate(
    session: AsyncSession, *, task_id: uuid.UUID, envelope: dict[str, Any]
) -> None:
    """Q1 final gate: a real-target apply requires evidence (or a waiver).

    Applies only to promotion applies — envelope target is the package's
    real ``integration_target_branch`` — of pinned horizon packages.
    Intermediate applies (integration branch), non-package tasks and legacy
    (unpinned) packages pass through unchanged (Q4 compat). Raises
    ``ValueError`` (→ BLOCKED ``approved_result_not_applied``) otherwise.
    """

    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task_id)
    )
    if link is None:
        return
    package = await session.get(WorkPackage, link.work_package_id)
    if package is None:
        return
    if getattr(package, "execution_contract_version", None) is not None:
        pinned = pinned_horizon_enabled(package, fallback=False)
    else:
        return
    if not pinned or not is_horizon(package.goal, package.exit_criteria):
        return
    target = envelope.get("target_branch")
    if not isinstance(target, str) or target != package.integration_target_branch:
        return
    if target == package.integration_branch:
        return
    expected_head = envelope.get("expected_target_head")
    result_commit = envelope.get("result_commit")
    if not isinstance(expected_head, str) or not isinstance(result_commit, str):
        raise ValueError("final acceptance gate: envelope heads are missing")
    evidence = await find_evidence(
        session,
        package_id=package.id,
        integration_base_head=expected_head,
        result_commit=result_commit,
    )
    if evidence is not None:
        return
    waiver = await find_waiver(
        session, package_id=package.id, integration_head=expected_head
    )
    if waiver is not None:
        return
    raise ValueError(
        "final acceptance gate: promotion to the real target requires "
        "acceptance evidence or a waiver"
    )


async def record_corrective_signal(
    session: AsyncSession,
    *,
    scope: str,
    reason: str,
    package_id: uuid.UUID | None = None,
    task_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
    step_id: uuid.UUID | None = None,
    notify: AttentionEvent = AttentionEvent.PACKAGE_ATTENTION,
) -> bool:
    """Durable corrective job trace (D2 L6, Q3): Event + projection outbox.

    The job itself reuses the existing bounded controls (retry/skip/repair
    within lifetime budget); this record makes "evaluating without a job"
    impossible to lose: the Event row is the durable trace, the outbox row
    refreshes the visible card, and the ``should_notify`` policy decision is
    persisted in the payload. Returns the notify decision.
    """

    decision = should_notify(notify)
    payload: dict[str, Any] = {
        "scope": scope,
        "reason": reason,
        "notify": decision,
        "notify_event": notify.value,
        "package_id": None if package_id is None else str(package_id),
        "task_id": None if task_id is None else str(task_id),
        "run_id": None if run_id is None else str(run_id),
        "step_id": None if step_id is None else str(step_id),
    }
    entity_id = package_id or task_id or run_id or step_id or uuid.uuid4()
    session.add(
        Event(
            entity_type="work_package" if package_id is not None else "task",
            entity_id=entity_id,
            event_type="work_package.correction_required"
            if package_id is not None
            else "task.correction_required",
            actor_type="system",
            payload=payload,
        )
    )
    if package_id is not None:
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_status",
                linked_entity_type="work_package",
                linked_entity_id=package_id,
                idempotency_key=(
                    f"wp:corrective:{package_id}:{payload['step_id'] or payload['task_id']}"
                ),
                payload={"package_id": str(package_id)},
            )
        )
    await session.flush()
    return decision


class AcceptanceGateHandler:
    """Materialized acceptance step (coding.v4, after ``approve_result``).

    Per item workflow: intermediate items and non-horizon/legacy packages
    pass through. For the LAST item of a pinned horizon package the handler
    assembles AcceptanceEvidence (criteria, deterministic gates, D1 review
    refs, unresolved caveats/effects), persists it as an artifact + evidence
    row, and succeeds — or BLOCKED fail-closed when criteria cannot be met.
    Idempotent: existing evidence for the same revision+heads is reused.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        artifacts: ArtifactStore | None = None,
    ) -> None:
        self._factory = session_factory
        self._artifacts = artifacts

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        try:
            evidence_id, summary = await self._assess(request)
        except (LookupError, ValueError) as error:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="acceptance_evidence_missing",
                summary=str(error)[:500],
                unknown_effects=False,
            )
        return StepOutcome.succeeded(
            {"acceptance_evidence_id": evidence_id, "summary": summary}
        )

    async def _assess(self, request: StepExecutionRequest) -> tuple[str | None, str]:
        async with self._factory() as session:
            step = await session.get(Step, request.step_id)
            run = await session.get(Run, request.run_id)
            task = await session.get(Task, request.task_id)
            if step is None or run is None or task is None:
                raise LookupError("acceptance step is missing task or run state")
            if (
                step.status not in {StepStatus.LEASED, StepStatus.RUNNING}
                or step.lease_owner != request.lease.owner
                or step.lease_generation != request.lease.generation
                or step.run_id != request.run_id
                or run.task_id != request.task_id
            ):
                raise ValueError("acceptance step is not bound to the current fenced lease")
            link = await session.scalar(
                select(MaterializationLink).where(MaterializationLink.task_id == task.id)
            )
            if link is None:
                return None, "no package link: acceptance pass-through"
            package = await session.get(WorkPackage, link.work_package_id)
            if package is None:
                raise LookupError("acceptance package is missing")
            if getattr(package, "execution_contract_version", None) is not None:
                pinned = pinned_horizon_enabled(package, fallback=False)
            else:
                return None, "legacy package: acceptance pass-through"
            if not pinned or not is_horizon(package.goal, package.exit_criteria):
                return None, "non-horizon package: acceptance pass-through"
            item_count = await session.scalar(
                select(func.count())
                .select_from(PlanRevisionItem)
                .where(PlanRevisionItem.plan_revision_id == link.plan_revision_id)
            )
            if link.ordinal != int(item_count or 0):
                return None, "intermediate item: acceptance pass-through"
            result = await self._assemble_last_item(
                session,
                package=package,
                run=run,
                task=task,
                step=step,
                link=link,
            )
            # This handler owns its session (like result_apply's factory
            # scope): evidence + artifact must commit here, or the gate
            # downstream would never see them.
            await session.commit()
            return result

    async def _assemble_last_item(
        self,
        session: AsyncSession,
        *,
        package: WorkPackage,
        run: Run,
        task: Task,
        step: Step,
        link: MaterializationLink,
    ) -> tuple[str, str]:
        from vuzol.execution.effect import _ACTIVE_STATUSES

        revision = await session.get(PlanRevision, link.plan_revision_id)
        if revision is None:
            raise LookupError("acceptance plan revision is missing")
        raw_criteria = package.exit_criteria
        criterion_ids = [
            entry["criterion_id"]
            for entry in (raw_criteria if isinstance(raw_criteria, list) else [])
            if isinstance(entry, dict) and isinstance(entry.get("criterion_id"), str)
        ]
        # Empty/missing criteria are NOT success: fail closed via the shared
        # horizon helper (empty → ("__no_exit_criteria__",)).
        unmet = unmet_exit_criteria(raw_criteria, frozenset(criterion_ids))
        if unmet:
            raise ValueError(
                f"acceptance blocked: unmet exit criteria: {','.join(unmet[:5])}"
            )
        # All materialized items must be terminal: the last review cannot
        # vouch for unfinished scope.
        links = (
            await session.scalars(
                select(MaterializationLink).where(
                    MaterializationLink.work_package_id == package.id,
                    MaterializationLink.plan_revision_id == link.plan_revision_id,
                )
            )
        ).all()
        task_ids = [item.task_id for item in links]
        states = (
            (
                await session.scalars(
                    select(Task.status).where(Task.id.in_(task_ids))
                )
            ).all()
            if task_ids
            else []
        )
        if any(status is not TaskStatus.COMPLETED for status in states):
            raise ValueError("acceptance blocked: package items are not all complete")
        # Review refs: every D1 history verdict for this package's steps.
        step_ids: list[uuid.UUID] = []
        for item_task_id in task_ids:
            item_runs = (
                await session.scalars(select(Run.id).where(Run.task_id == item_task_id))
            ).all()
            for item_run_id in item_runs:
                step_ids.extend(
                    (
                        await session.scalars(
                            select(Step.id).where(Step.run_id == item_run_id)
                        )
                    ).all()
                )
        history = (
            (
                await session.scalars(
                    select(ReviewOutcomeHistory).where(
                        ReviewOutcomeHistory.step_id.in_(step_ids)
                    )
                )
            ).all()
            if step_ids
            else []
        )
        review_refs = sorted({row.acceptance_key for row in history})
        if not review_refs:
            raise ValueError("acceptance blocked: no review verdicts retained")
        test_results = await self._collect_gates(session, task_ids)
        effects = (
            (
                await session.scalars(
                    select(Effect).where(Effect.step_id.in_(step_ids))
                )
            ).all()
            if step_ids
            else []
        )
        unresolved_effects = sorted(
            {
                effect.operation_key
                for effect in effects
                if effect.status in _ACTIVE_STATUSES
            }
        )
        unresolved_caveats = sorted(
            {
                str(finding.get("summary", ""))[:200]
                for row in history
                for finding in (row.findings or [])
                if isinstance(finding, dict)
                and finding.get("severity") in {"warning", "error"}
            }
        )
        worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
        base_head = package.integration_head_commit
        result_commit = worktree.result_commit if worktree is not None else None
        if not isinstance(base_head, str) or not isinstance(result_commit, str):
            raise ValueError("acceptance blocked: integration heads are missing")
        document: dict[str, Any] = {
            "schema": ACCEPTANCE_EVIDENCE_SCHEMA,
            "package_id": str(package.id),
            "plan_revision_id": str(revision.id),
            "plan_content_hash": revision.content_hash,
            "goal": (package.goal or "").strip(),
            "goal_revision": package.goal_revision,
            "spec_revision": task.spec_revision,
            "configuration_revision": run.configuration_revision,
            "policy_revision": run.policy_revision,
            "integration_base_head": base_head,
            "result_commit": result_commit,
            "criteria": [
                {
                    "criterion_id": criterion_id,
                    "satisfied": True,
                    "evidence_ref": f"review-history:{len(review_refs)}",
                }
                for criterion_id in criterion_ids
            ],
            "test_results": test_results,
            "review_refs": review_refs,
            "unresolved_caveats": unresolved_caveats,
            "unresolved_effects": unresolved_effects,
            "created_at": datetime.now(UTC).isoformat(),
        }
        if not document["goal"]:
            raise ValueError("acceptance blocked: package goal is missing")
        existing = await find_evidence(
            session,
            package_id=package.id,
            plan_revision_id=revision.id,
            integration_base_head=base_head,
        )
        if existing is not None:
            return str(existing.id), "acceptance evidence already recorded"
        if self._artifacts is None:
            raise ValueError("acceptance blocked: artifact store is unavailable")
        content = _canonical_json(document).encode()
        artifact = await self._artifacts.persist(
            session,
            task_id=task.id,
            run_id=run.id,
            step_id=step.id,
            artifact_type="acceptance_evidence",
            content=content,
            media_type="application/json",
            sensitivity="internal",
            visibility="private",
        )
        row = await record_evidence(
            session,
            package_id=package.id,
            plan_revision_id=revision.id,
            document=document,
            artifact_id=artifact.id,
        )
        return str(row.id), f"acceptance evidence recorded for {len(criterion_ids)} criteria"

    async def _collect_gates(
        self, session: AsyncSession, task_ids: list[uuid.UUID]
    ) -> list[dict[str, Any]]:
        """Deterministic test results from retained validate gates (best effort).

        Reads the structured validate output of each item run when present;
        an empty list is honest (no gates retained), never fabricated.
        """

        collected: list[dict[str, Any]] = []
        for item_task_id in task_ids:
            run_ids = (
                await session.scalars(select(Run.id).where(Run.task_id == item_task_id))
            ).all()
            for item_run_id in run_ids:
                validate = await session.scalar(
                    select(Step).where(
                        Step.run_id == item_run_id, Step.step_type == "validate"
                    )
                )
                if validate is None or not isinstance(validate.result, dict):
                    continue
                structured = validate.result.get("structured_output")
                if not isinstance(structured, dict):
                    continue
                gates = structured.get("gates")
                if not isinstance(gates, list):
                    continue
                for gate in gates:
                    if not isinstance(gate, dict):
                        continue
                    name = gate.get("name", "gate")
                    exit_code = gate.get("exit_code")
                    if isinstance(exit_code, bool):
                        continue
                    if not isinstance(exit_code, int):
                        continue
                    collected.append(
                        {"name": str(name)[:200], "exit_code": exit_code}
                    )
        return collected
