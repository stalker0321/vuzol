"""Read execution identity from the attempt, never from today's model defaults."""

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import Step, SupervisedProcess, UsageRecord


@dataclass(frozen=True)
class ExecutionIdentity:
    profile_id: str
    account_id: str | None = None
    model: str | None = None
    effort: str | None = None


async def step_execution_identity(session: AsyncSession, step: Step) -> ExecutionIdentity:
    profile_id = step.executor_profile_id or "не назначен"
    metadata = {}
    step_id = getattr(step, "id", None)
    if isinstance(step_id, uuid.UUID):
        process = await session.scalar(
            select(SupervisedProcess)
            .where(
                SupervisedProcess.step_id == step_id,
                SupervisedProcess.profile_id == profile_id,
            )
            .order_by(SupervisedProcess.created_at.desc(), SupervisedProcess.id.desc())
            .limit(1)
        )
        if process is not None:
            metadata = process.runtime_metadata or {}
    result = step.result if isinstance(step.result, dict) else {}
    payload = getattr(step, "payload", None)
    payload = payload if isinstance(payload, dict) else {}
    model = _text(result.get("model")) or _text(metadata.get("executor_model"))
    model = model or _text(payload.get("executor_model_override"))
    if model is None and isinstance(step_id, uuid.UUID):
        model = await session.scalar(
            select(UsageRecord.model)
            .where(UsageRecord.step_id == step_id, UsageRecord.profile_id == profile_id)
            .order_by(UsageRecord.created_at.desc(), UsageRecord.id.desc())
            .limit(1)
        )
    return ExecutionIdentity(
        profile_id=profile_id,
        account_id=_text(metadata.get("account_id")),
        model=model,
        effort=_text(metadata.get("executor_reasoning_effort"))
        or _text(payload.get("executor_reasoning_effort")),
    )


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None
