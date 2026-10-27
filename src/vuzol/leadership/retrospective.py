# ruff: noqa: RUF001
"""Transactional enqueue and fenced, retryable retrospective processing."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from contextlib import suppress
from typing import Literal

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.domain import DomainError
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import (
    TurnInvocationContext,
    current_turn,
    invocation_current,
)
from vuzol.leadership.model import LeadModelUnavailable
from vuzol.leadership.notebooks import apply_memory_command
from vuzol.leadership.persistence import LeadershipTurn, ManagedGoal
from vuzol.leadership.retrospective_input import retrospective_input
from vuzol.leadership.retrospective_model import RetrospectiveModel, RetrospectiveResponse
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import MaterializationLink, Run, TransactionalOutbox, WorkPackage
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import DeliveryStatus
from vuzol.telegram.projections import enqueue_chat_notice

DESTINATION = "goal_retrospective"
_LOG = logging.getLogger(__name__)


class RetrospectiveJob(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    outcome: Literal["accepted", "cancelled"]
    reason: str | None = None
    applied: bool = False


async def enqueue_retrospective(
    session: AsyncSession,
    goal: ManagedGoal,
    *,
    outcome: Literal["accepted", "cancelled"],
    reason: str | None = None,
) -> None:
    if outcome == "cancelled":
        worked = await session.scalar(
            select(WorkPackage.id)
            .where(
                WorkPackage.managed_goal_id == goal.id,
                WorkPackage.project_id == goal.project_id,
                or_(
                    WorkPackage.start_generation.is_not(None),
                    select(Run.id)
                    .join(MaterializationLink, MaterializationLink.task_id == Run.task_id)
                    .where(
                        MaterializationLink.work_package_id == WorkPackage.id,
                        Run.started_at.is_not(None),
                    )
                    .exists(),
                ),
            )
            .limit(1)
        )
        if worked is None:
            return
    await session.execute(
        insert(TransactionalOutbox)
        .values(
            destination=DESTINATION,
            operation_type="retrospect_goal",
            linked_entity_type="managed_goal",
            linked_entity_id=goal.id,
            idempotency_key=f"lead:retrospective:{goal.id}",
            payload=RetrospectiveJob(outcome=outcome, reason=reason).model_dump(),
            status=DeliveryStatus.PENDING,
        )
        .on_conflict_do_nothing(constraint="uq_outbox_idempotency")
    )


async def apply_retrospective(
    session: AsyncSession, goal: ManagedGoal, job_id: uuid.UUID, response: RetrospectiveResponse
) -> None:
    units = MemoryUnitRepository(session)
    for index, decision in enumerate(response.decisions):
        await units.create_decision(
            project_id=goal.project_id,
            goal_id=goal.id,
            text=decision.render(),
            details=decision.model_dump(),
            extraction_identity=f"lead-decision:{job_id}:{index}",
        )
    changes = {"added": 0, "replaced": 0, "forgotten": 0, "skipped": 0}
    for index, action in enumerate(response.notebook):
        payload = action.model_dump(exclude={"action"})
        try:
            async with session.begin_nested():
                await apply_memory_command(
                    session,
                    goal,
                    LeadCommand(
                        name=action.action,
                        project_id=goal.project_id,
                        goal_id=goal.id,
                        idempotency_key=f"retrospective:{job_id}:{index}",
                        payload=payload,
                    ),
                )
        except DomainError as error:
            changes["skipped"] += 1
            _LOG.info("Retrospective %s notebook action %s skipped: %s", job_id, index, error.code)
        else:
            key = (
                "forgotten"
                if action.action == "forget"
                else "replaced"
                if payload.get("replaces")
                else "added"
            )
            changes[key] += 1
    titles = "".join(f"\n• {decision.title}" for decision in response.decisions)
    await enqueue_chat_notice(
        session,
        chat_id=goal.source_chat_id,
        thread_id=goal.source_thread_id,
        key=f"lead:retrospective-summary:{job_id}",
        text=(
            f"📒 Итоги цели: {response.summary}{titles}\n"
            f"Блокнот: +{changes['added']}, ~{changes['replaced']} заменено, "
            f"−{changes['forgotten']} забыто, пропущено {changes['skipped']}."
        ),
    )


class GoalRetrospectiveConsumer:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        model: RetrospectiveModel,
        enabled: Callable[[], bool] = lambda: True,
        timeout_seconds: int = 60,
        max_attempts: int = 5,
        retry_min_seconds: float = 1,
        retry_max_seconds: float = 60,
    ) -> None:
        self._factory, self._owner, self._model = factory, owner, model
        self._enabled, self._timeout = enabled, timeout_seconds
        self._max_attempts = max_attempts
        self._retry_min, self._retry_max = retry_min_seconds, retry_max_seconds

    async def _locked_item(
        self, session: AsyncSession, token: OutboxLeaseToken
    ) -> TransactionalOutbox:
        item = await session.scalar(
            select(TransactionalOutbox)
            .where(
                TransactionalOutbox.id == token.item_id,
                TransactionalOutbox.status == DeliveryStatus.LEASED,
                TransactionalOutbox.lease_owner == token.owner,
                TransactionalOutbox.lease_generation == token.generation,
                TransactionalOutbox.lease_expires_at > func.now(),
            )
            .with_for_update()
        )
        if item is None:
            raise LeaseLost(f"retrospective lease lost: {token.item_id}")
        return item

    async def process_one(self) -> bool:
        if not self._enabled():
            return False
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._timeout + 60,
                allowed_destinations=frozenset({DESTINATION}),
            )
        if token is None:
            return False
        invocation_id = uuid.uuid5(token.item_id, str(token.generation))
        try:
            async with self._factory.begin() as session:
                item = await self._locked_item(session, token)
                job = RetrospectiveJob.model_validate(item.payload)
                # This receipt survives operational replay, including empty model results.
                if job.applied:
                    await complete_outbox_item(session, token)
                    return True
                if item.attempt_count > self._max_attempts:
                    raise LeadModelUnavailable("retrospective_attempt_limit")
                goal = await session.get(ManagedGoal, item.linked_entity_id)
                if goal is None:
                    raise LeadModelUnavailable("retrospective_goal_missing")
                goal_id, project_id = goal.id, goal.project_id
                context = await retrospective_input(
                    session, goal, outcome=job.outcome, reason=job.reason
                )
                # Reuse the durable inference/CLI supervision boundary without leasing
                # or mutating the terminal goal. Each outbox attempt has its own turn.
                session.add(
                    LeadershipTurn(
                        id=invocation_id,
                        goal_id=goal.id,
                        wakeup_id=invocation_id,
                        lease_epoch=token.generation,
                        goal_version=goal.version,
                        scope_revision=goal.scope_revision,
                        fence_epoch=goal.fence_epoch,
                        status="claimed",
                        owner=token.owner,
                    )
                )
                authority = TurnInvocationContext(
                    claim=TurnClaim(
                        goal.id, token.item_id, token.owner, token.generation, token.generation
                    ),
                    turn_id=invocation_id,
                    project_id=goal.project_id,
                    version=goal.version,
                    scope_revision=goal.scope_revision,
                    fence_epoch=goal.fence_epoch,
                    retrospective=True,
                    enabled=self._enabled,
                )
            binding = current_turn.set(authority)
            try:
                async with asyncio.timeout(self._timeout + 5):
                    response = await self._model.retrospect(
                        project_id,
                        context,
                        invocation_id=invocation_id,
                    )
                    # Validate even injected models before the first memory write.
                    response = RetrospectiveResponse.model_validate(response)
            finally:
                current_turn.reset(binding)
            async with self._factory.begin() as session:
                item = await self._locked_item(session, token)
                if not await invocation_current(session, authority):
                    raise LeaseLost(f"retrospective authority lost: {token.item_id}")
                goal = await session.get(ManagedGoal, goal_id)
                assert goal is not None
                await apply_retrospective(session, goal, token.item_id, response)
                turn = await session.get(LeadershipTurn, invocation_id)
                assert turn is not None
                turn.status = "applied"
                item.payload = job.model_copy(update={"applied": True}).model_dump()
                await complete_outbox_item(session, token)
        except LeaseLost:
            _LOG.info("Retrospective lease lost: %s", token.item_id)
        except Exception as error:
            category = (
                str(error)[:100]
                if isinstance(error, LeadModelUnavailable)
                else type(error).__name__
            )
            with suppress(LeaseLost):
                async with self._factory.begin() as session:
                    item = await self._locked_item(session, token)
                    turn = await session.get(LeadershipTurn, invocation_id)
                    if turn is not None:
                        turn.status = "lost"
                    if item.attempt_count >= self._max_attempts:
                        await dead_letter_outbox_item(session, token, error_category=category)
                        _LOG.warning("Retrospective %s dead-lettered: %s", token.item_id, category)
                    else:
                        await retry_outbox_item(
                            session,
                            token,
                            error_category=category,
                            delay_seconds=min(
                                self._retry_max,
                                self._retry_min * 2 ** min(item.attempt_count - 1, 20),
                            ),
                        )
        return True
