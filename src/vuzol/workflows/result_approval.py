"""Immutable approval envelopes for retained coding results."""

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Approval, Run, Step, Worktree
from vuzol.storage.types import ApprovalStatus

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
    predecessors = approval_step.dependency_metadata.get("predecessor_ordinals", [])
    if len(predecessors) != 1:
        raise ValueError("result approval requires exactly one predecessor")
    source_step = steps_by_ordinal[int(predecessors[0])]
    result = source_step.result or {}
    manifest = result.get("structured_output")
    if not isinstance(manifest, dict):
        raise ValueError("result approval requires a finalized worker manifest")
    worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
    if (
        worktree is None
        or worktree.result_commit is None
        or worktree.diff_hash is None
        or manifest.get("result_commit") != worktree.result_commit
        or manifest.get("base_commit") != worktree.base_commit
    ):
        raise ValueError("retained result does not match the finalized worker manifest")
    gates = manifest.get("gates")
    if (
        not isinstance(gates, list)
        or not gates
        or any(not isinstance(gate, dict) or gate.get("exit_code") != 0 for gate in gates)
    ):
        raise ValueError("result approval requires passing trusted gates")
    summary = result.get("implementation_summary")
    if not isinstance(summary, str) or not summary.strip():
        summary = "The requested change was implemented and passed all configured checks."
    summary = summary.strip()[:2_000]
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
        "gates": gates,
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
    return approval


def verified_envelope(step: Step, approval: Approval) -> dict[str, Any]:
    raw = step.payload.get("action_envelope")
    if not isinstance(raw, dict) or envelope_hash(raw) != approval.action_envelope_hash:
        raise ValueError("approval action envelope is missing or has changed")
    if raw.get("step_id") != str(step.id):
        raise ValueError("approval action envelope targets another step")
    return raw
