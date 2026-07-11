"""Approval, evidence, usage, configuration, and execution-resource repositories."""

import uuid
from typing import Any, cast

from sqlalchemy import CursorResult, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.errors import StorageError
from vuzol.storage.models import (
    Approval,
    Artifact,
    ClarificationDecision,
    ConfigurationRevision,
    Interpretation,
    ProfileHealthObservation,
    RoutingDecision,
    SupervisedProcess,
    UsageRecord,
    ValidationResult,
    Worktree,
)
from vuzol.storage.types import ApprovalStatus


class ModelRepository:
    """Storage-internal persistence for typed SQLAlchemy evidence models."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        model: Artifact
        | UsageRecord
        | Interpretation
        | ClarificationDecision
        | ValidationResult
        | RoutingDecision
        | ProfileHealthObservation
        | ConfigurationRevision
        | Worktree
        | SupervisedProcess,
    ) -> uuid.UUID:
        self._session.add(model)
        await self._session.flush()
        return model.id


class ApprovalRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, approval: Approval) -> uuid.UUID:
        self._session.add(approval)
        await self._session.flush()
        return approval.id

    async def consume(
        self, *, approval_id: uuid.UUID, token_hash: str, deciding_user_id: int
    ) -> None:
        statement = (
            update(Approval)
            .where(
                Approval.id == approval_id,
                Approval.token_hash == token_hash,
                Approval.status == ApprovalStatus.PENDING,
                Approval.expires_at > func.now(),
            )
            .values(
                status=ApprovalStatus.CONSUMED,
                decided_at=func.now(),
                consumed_at=func.now(),
                deciding_user_id=deciding_user_id,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        if result.rowcount != 1:
            raise StorageError("approval is invalid, expired, or already consumed")
