"""D1 work identity helpers: WorkAttempt lineage, versioned TaskSpec, outcome history.

All writers are additive and append-only:

- ``record_work_attempt`` creates a NEW row per repair/retry/takeover/REDO;
  closed attempts are never mutated (``close_work_attempt`` raises on a
  closed row). ``attempt_no`` is monotonic within ``step_id`` and is work
  lineage — never conflated with ``lease_generation`` or ``provider_attempt``
  (ADR-A01.4).
- ``snapshot_task_spec`` versions the spec separately from the mutating
  ``task_draft``; ``Task.spec_revision`` is the current pointer.
- ``record_review_outcome`` persists review verdicts (including BLOCKED ones
  that never reach ``Step.result``) into history keyed by the acceptance key.
  History is evidence retention only — never proof of a past review.
- ``takeover`` is a reserved ``attempt_kind`` value with no production
  writer yet (lead Q7); the enum already carries it.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    Approval,
    ConversationTurn,
    MaterializationLink,
    PlanRevisionItem,
    ReviewOutcomeHistory,
    Task,
    TaskSpecRevision,
    WorkAttempt,
)

REVIEW_OUTCOME_KEY_SCHEMA = "review-outcome-v1"


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def spec_revision_for(spec: dict[str, Any]) -> str:
    """Content-addressed spec revision for a task draft snapshot."""

    return hashlib.sha256(_canonical_json(spec).encode()).hexdigest()


async def next_attempt_no(session: AsyncSession, step_id: uuid.UUID) -> int:
    current = await session.scalar(
        select(func.max(WorkAttempt.attempt_no)).where(WorkAttempt.step_id == step_id)
    )
    return int(current or 0) + 1


async def latest_attempt(session: AsyncSession, step_id: uuid.UUID) -> WorkAttempt | None:
    row = await session.scalar(
        select(WorkAttempt)
        .where(WorkAttempt.step_id == step_id)
        .order_by(WorkAttempt.attempt_no.desc())
        .limit(1)
    )
    if row is None:
        return None
    assert isinstance(row, WorkAttempt)
    return row


async def resolve_stable_item(session: AsyncSession, task_id: uuid.UUID) -> uuid.UUID | None:
    """Logical item identity for an attempt chain (lead Q13).

    Materialized package tasks resolve to their ``WorkItemDraft.id`` via the
    materialization link; other tasks are their own logical item (``task_id``).
    ``tasks.id`` and ``MaterializationLink`` rows are only read, never
    rewritten here.
    """

    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task_id)
    )
    if link is None:
        return task_id
    item = await session.scalar(
        select(PlanRevisionItem).where(PlanRevisionItem.id == link.plan_revision_item_id)
    )
    if item is None or item.item_id is None:
        return task_id
    return item.item_id


async def record_work_attempt(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    attempt_kind: str,
    purpose: str,
    parent_attempt_id: uuid.UUID | None = None,
    stable_item_id: uuid.UUID | None = None,
    plan_revision_id: uuid.UUID | None = None,
    item_id: uuid.UUID | None = None,
    horizon_id: uuid.UUID | None = None,
    executor_profile_id: str | None = None,
    executor_model: str | None = None,
    node_id: str | None = None,
    lease_owner: str | None = None,
    lease_generation: int = 0,
    intent_revision: str | None = None,
    outcome: str = "running",
    usage_ref: str | None = None,
    cost_known: bool = True,
    prior_candidate_hash: str | None = None,
    prior_review_summary: str | None = None,
) -> WorkAttempt:
    """Create a new lineage row; never mutates existing rows."""

    attempt = WorkAttempt(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        plan_revision_id=plan_revision_id,
        item_id=item_id,
        stable_item_id=stable_item_id,
        horizon_id=horizon_id,
        attempt_no=await next_attempt_no(session, step_id),
        parent_attempt_id=parent_attempt_id,
        attempt_kind=attempt_kind,
        purpose=purpose,
        executor_profile_id=executor_profile_id,
        executor_model=executor_model,
        node_id=node_id,
        lease_owner=lease_owner,
        lease_generation=lease_generation,
        intent_revision=intent_revision,
        outcome=outcome,
        usage_ref=usage_ref,
        cost_known=cost_known,
        prior_candidate_hash=prior_candidate_hash,
        prior_review_summary=prior_review_summary,
    )
    session.add(attempt)
    await session.flush()
    return attempt


async def close_work_attempt(
    session: AsyncSession,
    attempt: WorkAttempt,
    *,
    outcome: str,
    output_hash: str | None = None,
    failure_category: str | None = None,
    failure_fingerprint: str | None = None,
    usage_ref: str | None = None,
) -> WorkAttempt:
    """Close an open attempt. Closed attempts are append-only (raises if closed)."""

    if attempt.closed_at is not None:
        raise ValueError("work attempt is already closed")
    attempt.outcome = outcome
    if output_hash is not None:
        attempt.output_hash = output_hash
    if failure_category is not None:
        attempt.failure_category = failure_category
    if failure_fingerprint is not None:
        attempt.failure_fingerprint = failure_fingerprint
    if usage_ref is not None:
        attempt.usage_ref = usage_ref
    # Python-side timestamp (not func.now()): the close guard re-reads
    # closed_at in the same session, and a server-side expression would
    # leave the attribute expired.
    attempt.closed_at = datetime.now(UTC)
    await session.flush()
    return attempt


async def snapshot_task_spec(
    session: AsyncSession,
    task: Task,
    *,
    source_turn_id: uuid.UUID | None = None,
) -> str:
    """Persist the current draft as a new spec revision (if changed).

    Returns the current ``spec_revision`` pointer (existing row is reused
    when the content is unchanged). Never rewrites history rows.
    """

    spec = dict(task.task_draft) if isinstance(task.task_draft, dict) else {}
    revision = spec_revision_for(spec)
    existing = await session.scalar(
        select(TaskSpecRevision).where(
            TaskSpecRevision.task_id == task.id,
            TaskSpecRevision.spec_revision == revision,
        )
    )
    if existing is None:
        session.add(
            TaskSpecRevision(
                task_id=task.id,
                spec_revision=revision,
                spec=spec,
                source_turn_id=source_turn_id or task.source_turn_id,
            )
        )
        await session.flush()
    task.spec_revision = revision
    return revision


async def validate_source_turn(
    session: AsyncSession, turn_id: uuid.UUID, *, session_id: uuid.UUID
) -> None:
    """Fail-closed session-membership check for a source turn (no write)."""

    owner = await session.scalar(
        select(ConversationTurn.session_id).where(ConversationTurn.id == turn_id)
    )
    if owner is None:
        raise ValueError("source turn does not exist")
    if owner != session_id:
        raise ValueError("source turn does not belong to the discussion session")


async def bind_task_source(
    session: AsyncSession,
    task: Task,
    turn_id: uuid.UUID,
    *,
    session_id: uuid.UUID,
) -> None:
    """Bind a typed source-turn ref with session-membership validation.

    Fail-closed like ``accept_decision``: a turn from another session raises
    instead of recording a false provenance link. The     original user turn is
    never replaced by generated text — only referenced.
    """

    await validate_source_turn(session, turn_id, session_id=session_id)
    task.source_turn_id = turn_id
    await session.flush()


def review_acceptance_key(
    verdict: dict[str, Any], *, approval_hash: str | None = None
) -> str:
    """Acceptance key for a review outcome (lead Q11).

    Reuses ``Approval.action_envelope_hash`` when an approval exists for the
    step; pre-approval outcomes use a content hash of the verdict — equally
    content-addressed, never a surrogate.
    """

    if approval_hash:
        return approval_hash
    return hashlib.sha256(
        f"{REVIEW_OUTCOME_KEY_SCHEMA}:{_canonical_json(verdict)}".encode()
    ).hexdigest()


async def record_review_outcome(
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    verdict: dict[str, Any],
) -> ReviewOutcomeHistory:
    """Persist a review verdict to history (idempotent on the acceptance key).

    Separate from the mutable ``Step.result``: BLOCKED verdicts that never
    reach ``Step.result`` are retained here with findings/diff/policy.
    """

    if not isinstance(verdict, dict):
        raise ValueError("review verdict payload is missing")
    approval_hash: str | None = None
    approval_id: uuid.UUID | None = None
    approval = await session.scalar(
        select(Approval).where(Approval.step_id == step_id).order_by(Approval.created_at.desc())
    )
    if approval is not None:
        approval_hash = approval.action_envelope_hash
        approval_id = approval.id
    key = review_acceptance_key(verdict, approval_hash=approval_hash)
    existing = await session.scalar(
        select(ReviewOutcomeHistory).where(ReviewOutcomeHistory.acceptance_key == key)
    )
    if existing is not None:
        return existing
    findings = verdict.get("findings")
    row = ReviewOutcomeHistory(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        acceptance_key=key,
        approval_id=approval_id,
        verdict=str(verdict.get("verdict", "blocked")),
        review_kind=verdict.get("review_kind"),
        risk=verdict.get("risk"),
        base_commit=verdict.get("base_commit"),
        result_commit=verdict.get("result_commit"),
        diff_hash=verdict.get("diff_hash"),
        findings=list(findings) if isinstance(findings, list) else [],
        summary=verdict.get("summary"),
        policy_revision=verdict.get("policy_revision"),
        partition_count=verdict.get("partition_count"),
        unknown_usage=bool(verdict.get("unknown_usage", False)),
    )
    session.add(row)
    # No IntegrityError catch: the pre-check above makes this idempotent, and
    # step-lease fencing precludes concurrent duplicate commits. A genuinely
    # duplicated key fails closed instead of silently dropping evidence.
    await session.flush()
    return row
