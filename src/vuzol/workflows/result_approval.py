"""Immutable approval envelopes for retained coding results."""

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import (
    Approval,
    Event,
    MaterializationLink,
    PlanRevisionItem,
    Run,
    Step,
    Worktree,
)
from vuzol.storage.types import ApprovalStatus, StepStatus
from vuzol.workflows.transitions import transition_step

RESULT_APPROVAL_SCHEMA = "result-approval.v1"
RESULT_APPROVAL_TTL = timedelta(days=7)


def envelope_hash(envelope: dict[str, Any]) -> str:
    payload = json.dumps(envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


async def ensure_result_approval(
    session: AsyncSession,
    *,
    run: Run,
    approval_step: Step,
    steps_by_ordinal: dict[int, Step],
) -> Approval | None:
    """Create the single approval record when an apply-result step becomes ready."""

    if approval_step.payload.get("requested_action") != "apply_result":
        return None
    existing = await session.scalar(select(Approval).where(Approval.step_id == approval_step.id))
    if existing is not None:
        return existing

    worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
    if worktree is None or worktree.result_commit is None or worktree.diff_hash is None:
        raise ValueError("result approval requires a retained measured worktree")

    evidence = _validation_evidence(steps_by_ordinal, worktree)
    gates = evidence["gates"]
    agent_checks = evidence["agent_checks"]
    summary = evidence["summary"]
    envelope: dict[str, Any] = {
        "schema_version": RESULT_APPROVAL_SCHEMA,
        "requested_action": "apply_result",
        "task_id": str(run.task_id),
        "run_id": str(run.id),
        "step_id": str(approval_step.id),
        "project_id": worktree.project_id,
        "repository_identity_hash": worktree.repository_identity_hash,
        "target_branch": worktree.default_branch,
        "expected_target_head": worktree.expected_target_head,
        "base_commit": worktree.base_commit,
        "result_commit": worktree.result_commit,
        "diff_hash": worktree.diff_hash,
        "agent_checks": agent_checks,
        "gates": gates,
        "changed_files": list(evidence["changed_files"]),
        "validation_evidence_hash": evidence["validation_evidence_hash"],
        "review_evidence": evidence["review_evidence"],
        "review_evidence_hash": evidence["review_evidence_hash"],
        "configuration_revision": run.configuration_revision,
        "policy_revision": run.policy_revision,
    }
    digest = envelope_hash(envelope)
    approval_id = uuid.uuid4()
    token_hash = hashlib.sha256(f"{approval_id}:{digest}".encode()).hexdigest()
    approval = Approval(
        id=approval_id,
        step_id=approval_step.id,
        action_envelope_hash=digest,
        requested_action="apply_result",
        normalized_target=f"{worktree.project_id}:{worktree.default_branch}",
        human_summary=summary,
        token_hash=token_hash,
        status=ApprovalStatus.PENDING,
        expires_at=datetime.now(UTC) + RESULT_APPROVAL_TTL,
    )
    session.add(approval)
    approval_step.payload = {
        **approval_step.payload,
        "approval_id": str(approval.id),
        "action_envelope": envelope,
    }
    approval_step.external_idempotency_key = f"apply-result:{digest}"
    await session.flush()
    if await _plan_approval_covers_intermediate_result(session, run.task_id):
        approval.status = ApprovalStatus.APPROVED
        approval.decided_at = func.now()
        await transition_step(
            session,
            approval_step,
            StepStatus.QUEUED,
            actor_type="workflow_manager",
            actor_id="approved_plan",
        )
        session.add(
            Event(
                entity_type="task",
                entity_id=run.task_id,
                event_type="result.auto_approved_by_plan",
                actor_type="workflow_manager",
                actor_id="approved_plan",
                payload={"approval_id": str(approval.id)},
            )
        )
    return approval


async def _plan_approval_covers_intermediate_result(
    session: AsyncSession, task_id: uuid.UUID
) -> bool:
    """Allow approved plans to apply safe intermediate results without a user pause."""

    link = (
        await session.execute(
            select(MaterializationLink).where(MaterializationLink.task_id == task_id)
        )
    ).scalar_one_or_none()
    if link is None:
        return False
    item_count = await session.scalar(
        select(func.count())
        .select_from(PlanRevisionItem)
        .where(PlanRevisionItem.plan_revision_id == link.plan_revision_id)
    )
    # Approving a package is the user's authorization to run its chain without
    # stopping after every retained result. The final item remains the single
    # human result-approval boundary. Item-level risk metadata still informs
    # planning/review, but must not silently turn an approved chain into a set
    # of per-item approvals.
    return link.ordinal < int(item_count or 0)


def verified_envelope(step: Step, approval: Approval) -> dict[str, Any]:
    raw = step.payload.get("action_envelope")
    if not isinstance(raw, dict) or envelope_hash(raw) != approval.action_envelope_hash:
        raise ValueError("approval action envelope is missing or has changed")
    if raw.get("step_id") != str(step.id):
        raise ValueError("approval action envelope targets another step")
    return raw


def _validation_evidence(steps_by_ordinal: dict[int, Step], worktree: Worktree) -> dict[str, Any]:
    """Collect gate evidence and summary from validate/review/execute predecessors."""

    ordered = [steps_by_ordinal[key] for key in sorted(steps_by_ordinal)]
    validate = next(
        (
            step
            for step in ordered
            if step.step_type == "validate" and step.status is StepStatus.COMPLETED
        ),
        None,
    )
    review_steps = [step for step in ordered if step.step_type == "review"]
    review = next(
        (
            step
            for step in ordered
            if step.step_type == "review" and step.status is StepStatus.COMPLETED
        ),
        None,
    )
    execute = next(
        (
            step
            for step in reversed(ordered)
            if step.step_type in {"execute_code", "execute_agent"}
            and step.status is StepStatus.COMPLETED
        ),
        None,
    )

    source = validate
    if source is None:
        # Fall back to any completed predecessor with structured validation output.
        for step in reversed(ordered):
            if step.status is not StepStatus.COMPLETED:
                continue
            result = step.result if isinstance(step.result, dict) else {}
            structured = result.get("structured_output")
            if (
                isinstance(structured, dict)
                and structured.get("result_commit") == worktree.result_commit
                and isinstance(structured.get("gates"), list)
                and structured.get("gates")
            ):
                source = step
                break
    if source is None:
        raise ValueError("result approval requires a completed validate step")

    result = source.result if isinstance(source.result, dict) else {}
    manifest = result.get("structured_output")
    if not isinstance(manifest, dict):
        raise ValueError("result approval requires structured validation output")
    if (
        manifest.get("result_commit") != worktree.result_commit
        or manifest.get("base_commit") != worktree.base_commit
    ):
        raise ValueError("retained result does not match the finalized validation evidence")
    gates = manifest.get("gates")
    if (
        not isinstance(gates, list)
        or not gates
        or any(not isinstance(gate, dict) or gate.get("exit_code") != 0 for gate in gates)
    ):
        raise ValueError("result approval requires passing trusted gates")

    review_evidence: dict[str, Any] | None = None
    review_evidence_hash: str | None = None
    if review_steps and review is None:
        raise ValueError("result approval requires the configured review step to complete")
    if review is not None:
        review_result = review.result if isinstance(review.result, dict) else {}
        review_manifest = review_result.get("structured_output")
        if not isinstance(review_manifest, dict):
            raise ValueError("result approval requires structured review output")
        if (
            review_manifest.get("base_commit") != worktree.base_commit
            or review_manifest.get("result_commit") != worktree.result_commit
            or review_manifest.get("diff_hash") != worktree.diff_hash
        ):
            raise ValueError("review evidence does not match the retained result")
        verdict = review_manifest.get("verdict")
        if verdict not in {"pass", "pass_with_warnings"}:
            raise ValueError("result approval requires a passing review verdict")
        findings = review_manifest.get("findings", [])
        if not isinstance(findings, list) or any(
            not isinstance(finding, dict) or finding.get("severity") in {"error", "blocker"}
            for finding in findings
        ):
            raise ValueError("passing review evidence contains blocking findings")
        warnings = _review_warnings(findings)
        review_evidence_hash = envelope_hash(review_manifest)
        review_evidence = {
            "schema_version": review_manifest.get("schema_version"),
            "verdict": verdict,
            "review_kind": review_manifest.get("review_kind"),
            "risk": review_manifest.get("risk"),
            "base_commit": review_manifest.get("base_commit"),
            "result_commit": review_manifest.get("result_commit"),
            "diff_hash": review_manifest.get("diff_hash"),
            "warnings": warnings,
            "evidence_hash": review_evidence_hash,
        }

    review_result = review.result if review and isinstance(review.result, dict) else {}
    execute_result = execute.result if execute and isinstance(execute.result, dict) else {}
    agent_checks = _agent_checks(execute_result)
    summary = None
    for candidate in (execute_result.get("implementation_summary"),):
        if isinstance(candidate, str) and candidate.strip():
            summary = " ".join(candidate.split()).strip()
            break
    if summary is None:
        changed_files = manifest.get("changed_files")
        changed_count = len(changed_files) if isinstance(changed_files, list) else 0
        summary = f"Изменено файлов: {changed_count}. Результат прошёл все настроенные проверки."
    return {
        "agent_checks": agent_checks,
        "gates": gates,
        "summary": summary[:2_000],
        "changed_files": tuple(
            str(path) for path in manifest.get("changed_files", []) if isinstance(path, str)
        ),
        "validation_evidence_hash": envelope_hash(manifest),
        "review_evidence": review_evidence,
        "review_evidence_hash": review_evidence_hash,
    }


def _agent_checks(execute_result: dict[str, Any]) -> list[dict[str, str | None]]:
    """Copy bounded provider claims without treating them as validation evidence."""

    structured = execute_result.get("structured_output")
    raw = structured.get("agent_checks") if isinstance(structured, dict) else None
    if not isinstance(raw, list):
        return []
    allowed_statuses = {"passed", "failed", "not_run", "unavailable"}
    checks: list[dict[str, str | None]] = []
    for item in raw[:12]:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        status = item.get("status")
        detail = item.get("detail")
        if (
            not isinstance(name, str)
            or not name.strip()
            or len(name) > 100
            or status not in allowed_statuses
            or (detail is not None and (not isinstance(detail, str) or len(detail) > 500))
        ):
            continue
        checks.append(
            {
                "name": " ".join(name.split()),
                "status": str(status),
                "detail": (
                    " ".join(detail.split()) if isinstance(detail, str) and detail.strip() else None
                ),
            }
        )
    return checks


def _review_warnings(findings: list[object]) -> list[dict[str, str | int | None]]:
    """Copy bounded approval-visible warnings from hash-bound review evidence."""

    warnings: list[dict[str, str | int | None]] = []
    for item in findings[:12]:
        if not isinstance(item, dict) or item.get("severity") != "warning":
            continue
        classification = item.get("classification")
        summary = item.get("summary")
        path = item.get("path")
        line = item.get("line")
        if (
            not isinstance(classification, str)
            or not classification.strip()
            or not isinstance(summary, str)
            or not summary.strip()
            or (path is not None and not isinstance(path, str))
            or (line is not None and (not isinstance(line, int) or isinstance(line, bool)))
        ):
            continue
        warnings.append(
            {
                "classification": " ".join(classification.split())[:100],
                "summary": " ".join(summary.split())[:500],
                "path": " ".join(path.split())[:500] if isinstance(path, str) else None,
                "line": line,
            }
        )
    return warnings
