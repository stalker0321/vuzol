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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.horizon import is_horizon, pinned_horizon_enabled, unmet_exit_criteria
from vuzol.execution.artifacts import ArtifactStore
from vuzol.review.domain import ExitCriterionSpec
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
        if not isinstance(ref, str):
            return ("evidence_review_ref_malformed",)
        # D1 history acceptance keys (64-hex) or retained step verdict refs
        # ("step-review:<uuid>") — both resolve to a stored review verdict.
        if len(ref) == 64:
            continue
        if ref.startswith("step-review:"):
            try:
                uuid.UUID(ref.split(":", 1)[1])
            except ValueError:
                return ("evidence_review_ref_malformed",)
            continue
        return ("evidence_review_ref_malformed",)
    return ()


def required_criterion_ids(exit_criteria: object) -> tuple[str, ...]:
    """Ids declared on the current goal. A colon would break evidence refs."""

    if not isinstance(exit_criteria, list):
        return ()
    ids: list[str] = []
    for entry in exit_criteria:
        if not isinstance(entry, dict):
            continue
        criterion_id = entry.get("criterion_id")
        if not isinstance(criterion_id, str):
            continue
        criterion_id = criterion_id.strip()
        if criterion_id and ":" not in criterion_id:
            ids.append(criterion_id)
    return tuple(ids)


def _bounded_text(entry: Mapping[str, Any], key: str, limit: int) -> str | None:
    raw = entry.get(key)
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    return text[:limit]


def exit_criterion_specs(exit_criteria: object) -> tuple[ExitCriterionSpec, ...]:
    """Keep each criterion's statement. An id alone is not the requirement."""

    if not isinstance(exit_criteria, list):
        return ()
    by_id: dict[str, Mapping[str, Any]] = {}
    for entry in exit_criteria:
        if not isinstance(entry, dict):
            continue
        raw_id = entry.get("criterion_id")
        if isinstance(raw_id, str) and raw_id.strip():
            by_id[raw_id.strip()] = entry
    specs: list[ExitCriterionSpec] = []
    for criterion_id in required_criterion_ids(exit_criteria):
        entry = by_id.get(criterion_id, {})
        observable = entry.get("observable")
        specs.append(
            ExitCriterionSpec(
                criterion_id=criterion_id,
                statement=_bounded_text(entry, "statement", 1000),
                evidence_kind=_bounded_text(entry, "evidence_kind", 100),
                observable=observable if isinstance(observable, bool) else None,
            )
        )
    return tuple(specs)


def unproven_criterion_ids(
    *,
    result_commit: str,
    required_ids: Sequence[str],
    gates: Sequence[Mapping[str, Any]],
    findings: Sequence[Mapping[str, Any]],
    owner_decisions: Sequence[Mapping[str, Any]] = (),
) -> tuple[str, ...]:
    """Required criteria that have no stored proof of this candidate."""

    missing: list[str] = []
    for criterion_id in required_ids:
        if (
            evidence_ref_for_criterion(
                criterion_id,
                result_commit=result_commit,
                test_results=gates,
                findings=findings,
                owner_decisions=owner_decisions,
            )
            is None
        ):
            missing.append(criterion_id)
    return tuple(missing)


def evidence_ref_for_criterion(
    criterion_id: str,
    *,
    result_commit: str,
    test_results: Sequence[Mapping[str, Any]],
    findings: Sequence[Mapping[str, Any]],
    owner_decisions: Sequence[Mapping[str, Any]] = (),
) -> str | None:
    """Bind one required criterion to a stored check of this exact candidate.

    The candidate commit is never written onto a gate or finding that was
    measured against a different commit. ``review-history:N`` and a model
    string ``human_accepted`` are not measurements.
    """

    for gate in test_results:
        if gate.get("name") != criterion_id or gate.get("result_commit") != result_commit:
            continue
        exit_code = gate.get("exit_code")
        if isinstance(exit_code, bool) or exit_code != 0:
            continue
        return f"gate:{criterion_id}:{result_commit}"
    for finding in findings:
        if finding.get("criterion_id") != criterion_id:
            continue
        if finding.get("result_commit") != result_commit:
            continue
        if finding.get("classification") != "criterion_pass":
            continue
        evidence = finding.get("evidence")
        if not isinstance(evidence, str) or not evidence.strip():
            continue
        if evidence.strip() == criterion_id:
            continue
        key = finding.get("acceptance_key")
        if not isinstance(key, str) or len(key) != 64:
            continue
        try:
            int(key, 16)
        except ValueError:
            continue
        return f"review-criterion:{key}:{criterion_id}:{result_commit}"
    for decision in owner_decisions:
        if decision.get("criterion_id") != criterion_id:
            continue
        if decision.get("result_commit") != result_commit:
            continue
        principal = decision.get("principal_user_id")
        reason = decision.get("reason")
        event_id = decision.get("event_id")
        if not isinstance(principal, int) or principal == 0:
            continue
        if not isinstance(reason, str) or not reason.strip():
            continue
        if not isinstance(event_id, str) or not event_id or ":" in event_id:
            continue
        return f"owner-criterion:{event_id}:{criterion_id}:{result_commit}"
    return None


def criteria_are_proven(
    document: object,
    *,
    result_commit: str,
    required_ids: Sequence[str],
    gates: Sequence[Mapping[str, Any]],
    findings: Sequence[Mapping[str, Any]],
    owner_decisions: Sequence[Mapping[str, Any]] = (),
) -> bool:
    """Every current required criterion cites a stored proof of this commit.

    A well-formed ``evidence_ref`` is only a citation. It counts when the same
    ref is produced from ``gates``, ``findings``, or an owner decision.
    """

    if not required_ids:
        return False
    if not isinstance(document, dict) or document.get("result_commit") != result_commit:
        return False
    criteria = document.get("criteria")
    if not isinstance(criteria, list):
        return False
    cited: dict[str, str] = {}
    for entry in criteria:
        if not isinstance(entry, dict) or entry.get("satisfied") is not True:
            continue
        criterion_id = entry.get("criterion_id")
        ref = entry.get("evidence_ref")
        if isinstance(criterion_id, str) and isinstance(ref, str):
            cited[criterion_id] = ref
    for criterion_id in required_ids:
        ref = evidence_ref_for_criterion(
            criterion_id,
            result_commit=result_commit,
            test_results=gates,
            findings=findings,
            owner_decisions=owner_decisions,
        )
        if ref is None or cited.get(criterion_id) != ref:
            return False
    return True


@dataclass(frozen=True, slots=True)
class StoredCriterionProofs:
    """Checks loaded from validate output, review history, and owner actions."""

    gates: tuple[dict[str, Any], ...]
    findings: tuple[dict[str, Any], ...]
    owner_decisions: tuple[dict[str, Any], ...]


async def load_stored_proofs(
    session: AsyncSession, *, package_id: uuid.UUID
) -> StoredCriterionProofs:
    """Resolve criterion proofs from retained rows, not from the evidence text."""

    links = (
        await session.scalars(
            select(MaterializationLink).where(MaterializationLink.work_package_id == package_id)
        )
    ).all()
    task_ids = [link.task_id for link in links]
    gates: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    if task_ids:
        run_ids = (await session.scalars(select(Run.id).where(Run.task_id.in_(task_ids)))).all()
        if run_ids:
            steps = (
                await session.scalars(
                    select(Step).where(Step.run_id.in_(run_ids), Step.step_type == "validate")
                )
            ).all()
            for validate in steps:
                if not isinstance(validate.result, dict):
                    continue
                structured = validate.result.get("structured_output")
                if not isinstance(structured, dict):
                    continue
                origin = structured.get("result_commit")
                raw_gates = structured.get("gates")
                if not isinstance(origin, str) or not isinstance(raw_gates, list):
                    continue
                for gate in raw_gates:
                    if not isinstance(gate, dict):
                        continue
                    exit_code = gate.get("exit_code")
                    if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                        continue
                    gates.append(
                        {
                            "name": str(gate.get("name", ""))[:200],
                            "exit_code": exit_code,
                            "result_commit": origin,
                            "step_id": str(validate.id),
                            "attempt": validate.attempt_count,
                        }
                    )
        history = (
            await session.scalars(
                select(ReviewOutcomeHistory).where(ReviewOutcomeHistory.task_id.in_(task_ids))
            )
        ).all()
        for row in history:
            if not isinstance(row.result_commit, str):
                continue
            for finding in row.findings or []:
                if isinstance(finding, dict):
                    findings.append(
                        {
                            **finding,
                            "acceptance_key": row.acceptance_key,
                            "result_commit": row.result_commit,
                        }
                    )
    decisions = (
        await session.scalars(
            select(Event).where(
                Event.entity_type == "work_package",
                Event.entity_id == package_id,
                Event.event_type == "acceptance.owner_criterion",
            )
        )
    ).all()
    owner: list[dict[str, Any]] = []
    for event in decisions:
        payload = event.payload if isinstance(event.payload, dict) else {}
        owner.append({**payload, "event_id": str(event.id)})
    return StoredCriterionProofs(tuple(gates), tuple(findings), tuple(owner))


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


def criterion_choice_token(criterion_id: str) -> str:
    """Short stable token for one criterion. The callback cannot carry the full id."""

    return hashlib.sha256(criterion_id.strip().encode()).hexdigest()[:8]


async def record_owner_criterion_acceptance(
    session: AsyncSession,
    *,
    package_id: uuid.UUID,
    criterion_id: str,
    result_commit: str,
    principal_user_id: int,
    reason: str,
) -> Event:
    """Owner accepts one criterion for one candidate. A model cannot do this."""

    if principal_user_id == 0 or not reason.strip() or not criterion_id.strip():
        raise ValueError("owner criterion acceptance requires a principal and a reason")
    if ":" in criterion_id:
        raise ValueError("criterion id cannot contain a colon")
    row = Event(
        entity_type="work_package",
        entity_id=package_id,
        event_type="acceptance.owner_criterion",
        actor_type="user",
        actor_id=str(principal_user_id),
        payload={
            "criterion_id": criterion_id.strip(),
            "result_commit": result_commit,
            "principal_user_id": principal_user_id,
            "reason": reason.strip(),
        },
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
    revision = await session.get(PlanRevision, link.plan_revision_id)
    document = evidence.evidence if evidence is not None else None
    revision_current = (
        evidence is not None
        and isinstance(document, dict)
        and revision is not None
        and evidence.plan_revision_id == link.plan_revision_id
        and document.get("plan_content_hash") == revision.content_hash
        and (
            package.goal_revision is None or document.get("goal_revision") == package.goal_revision
        )
    )
    proofs = await load_stored_proofs(session, package_id=package.id)
    if revision_current and criteria_are_proven(
        document,
        result_commit=result_commit,
        required_ids=required_criterion_ids(package.exit_criteria),
        gates=proofs.gates,
        findings=proofs.findings,
        owner_decisions=proofs.owner_decisions,
    ):
        return
    waiver = await find_waiver(session, package_id=package.id, integration_head=expected_head)
    if waiver is not None:
        return
    if evidence is not None and not revision_current:
        raise ValueError("final acceptance gate: evidence revision is stale")
    if evidence is not None:
        raise ValueError(
            "final acceptance gate: required criteria are not proven for this candidate"
        )
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


def _verdict_allows_progress(result: object) -> bool:
    """The retained review verdict lets the item promote (pass flavors only)."""

    if not isinstance(result, dict):
        return False
    return result.get("verdict") in {"pass", "pass_with_warnings"}


class AcceptanceGateHandler:
    """Materialized acceptance step (coding.v4, before ``approve_result``).

    Runs in every item workflow after ``publish_preview``: intermediate items
    and non-horizon/legacy packages pass through. For the LAST item of a
    pinned horizon package the handler assembles AcceptanceEvidence — prior
    items terminal, this item reviewed — and persists it, so the coming
    promotion apply finds the gate open. BLOCKED fail-closed when criteria
    cannot be met. Idempotent: existing evidence for the same
    revision+promotion-base is reused.
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
        return StepOutcome.succeeded({"acceptance_evidence_id": evidence_id, "summary": summary})

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
        criterion_ids = list(required_criterion_ids(raw_criteria))
        # Empty/missing criteria are NOT success: fail closed via the shared
        # horizon helper (empty → ("__no_exit_criteria__",)).
        unmet = unmet_exit_criteria(raw_criteria, frozenset(criterion_ids))
        if unmet:
            raise ValueError(f"acceptance blocked: unmet exit criteria: {','.join(unmet[:5])}")
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
        # REDO-1 order (acceptance BEFORE approve_result): the last item is
        # still running when its acceptance step executes. Prior items must
        # be terminal; the current item must already be reviewed (its review
        # step COMPLETED with a passing verdict) — otherwise the gate cannot
        # vouch for the scope it is about to promote.
        current_review_refs: list[str] = []
        for item in links:
            if item.task_id == task.id:
                review = await session.scalar(
                    select(Step).where(
                        Step.run_id == run.id,
                        Step.step_type == "review",
                        Step.status == StepStatus.COMPLETED,
                    )
                )
                if review is None or not _verdict_allows_progress(review.result):
                    raise ValueError("acceptance blocked: last item is not reviewed")
                current_review_refs.append(f"step-review:{review.id}")
                continue
            other = await session.get(Task, item.task_id)
            if other is None or other.status is not TaskStatus.COMPLETED:
                raise ValueError("acceptance blocked: package items are not all complete")
        # Review refs: every D1 history verdict for this package's steps.
        step_ids: list[uuid.UUID] = []
        for item_task_id in task_ids:
            item_runs = (
                await session.scalars(select(Run.id).where(Run.task_id == item_task_id))
            ).all()
            for item_run_id in item_runs:
                step_ids.extend(
                    (await session.scalars(select(Step.id).where(Step.run_id == item_run_id))).all()
                )
        history = (
            (
                await session.scalars(
                    select(ReviewOutcomeHistory).where(ReviewOutcomeHistory.step_id.in_(step_ids))
                )
            ).all()
            if step_ids
            else []
        )
        review_refs = sorted({row.acceptance_key for row in history} | set(current_review_refs))
        if not review_refs:
            raise ValueError("acceptance blocked: no review verdicts retained")
        proofs = await load_stored_proofs(session, package_id=package.id)
        test_results = list(proofs.gates)
        effects = (
            (await session.scalars(select(Effect).where(Effect.step_id.in_(step_ids)))).all()
            if step_ids
            else []
        )
        unresolved_effects = sorted(
            {effect.operation_key for effect in effects if effect.status in _ACTIVE_STATUSES}
        )
        unresolved_caveats = sorted(
            {
                str(finding.get("summary", ""))[:200]
                for row in history
                for finding in (row.findings or [])
                if isinstance(finding, dict) and finding.get("severity") in {"warning", "error"}
            }
        )
        worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
        # REDO-2 (Option A): evidence binds the promotion the coming apply
        # will present — the frozen promotion base (expected_target_head
        # contract), not the moving integration head. The gate matches the
        # envelope claim against this pair without reading mutable state.
        base_head = package.integration_base_commit
        result_commit = worktree.result_commit if worktree is not None else None
        if not isinstance(base_head, str) or not isinstance(result_commit, str):
            raise ValueError("acceptance blocked: integration heads are missing")
        criteria_rows: list[dict[str, Any]] = []
        missing: list[str] = []
        for criterion_id in criterion_ids:
            ref = evidence_ref_for_criterion(
                criterion_id,
                result_commit=result_commit,
                test_results=proofs.gates,
                findings=proofs.findings,
                owner_decisions=proofs.owner_decisions,
            )
            if ref is None:
                missing.append(criterion_id)
            criteria_rows.append(
                {
                    "criterion_id": criterion_id,
                    "satisfied": ref is not None,
                    "evidence_ref": ref,
                }
            )
        if missing:
            raise ValueError("acceptance blocked: criteria lack evidence: " + ",".join(missing[:5]))
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
            "criteria": criteria_rows,
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
        if (
            existing is not None
            and existing.result_commit == result_commit
            and criteria_are_proven(
                existing.evidence,
                result_commit=result_commit,
                required_ids=tuple(criterion_ids),
                gates=proofs.gates,
                findings=proofs.findings,
                owner_decisions=proofs.owner_decisions,
            )
        ):
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
