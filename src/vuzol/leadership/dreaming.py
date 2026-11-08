"""Nightly project notebook jobs with leased inference and atomic consolidation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any, cast
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.settings import ProjectLeadSettings
from vuzol.leadership.dreaming_model import DreamModel, DreamResponse, validate_dream
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import (
    TurnInvocationContext,
    current_turn,
    invocation_current,
)
from vuzol.leadership.model import LeadModelUnavailable, NotebookNote
from vuzol.leadership.notebooks import note_view
from vuzol.leadership.persistence import LeadershipTurn, ManagedGoal
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import MemoryUnit, TransactionalOutbox
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.repositories.memory_units import MemoryUnitRepository
from vuzol.storage.types import DeliveryStatus, MemoryScope, MemoryUnitStatus
from vuzol.telegram.projections import enqueue_chat_notice

DESTINATION = "project_dream"
_LOG = logging.getLogger(__name__)


class DreamJob(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    project_id: str
    local_date: str
    applied: bool = False
    fingerprint: str | None = None


def notebook_fingerprint(notes: Sequence[NotebookNote]) -> str:
    return hashlib.sha256(json.dumps(notes, sort_keys=True).encode()).hexdigest()


async def project_idle(session: AsyncSession, project_id: str, *, lock: bool = False) -> bool:
    statement = select(ManagedGoal).where(ManagedGoal.project_id == project_id)
    if lock:
        statement = statement.order_by(ManagedGoal.id).with_for_update()
    goals = list(await session.scalars(statement.execution_options(populate_existing=True)))
    now = await session.scalar(select(func.now()))
    assert now is not None
    return not any(goal.lease_until is not None and goal.lease_until > now for goal in goals)


async def last_dream(session: AsyncSession, project_id: str) -> TransactionalOutbox | None:
    return cast(
        TransactionalOutbox | None,
        await session.scalar(
            select(TransactionalOutbox)
            .where(
                TransactionalOutbox.destination == DESTINATION,
                TransactionalOutbox.payload["project_id"].astext == project_id,
                TransactionalOutbox.payload["applied"].as_boolean().is_(True),
            )
            .order_by(TransactionalOutbox.updated_at.desc(), TransactionalOutbox.id.desc())
            .limit(1)
        ),
    )


async def enqueue_dreams(
    session: AsyncSession,
    *,
    settings: ProjectLeadSettings,
    timezone: str,
    now: datetime | None = None,
) -> None:
    if not settings.enabled or not settings.dreaming_enabled:
        return
    local = (now or datetime.now(UTC)).astimezone(ZoneInfo(timezone))
    if local.hour < settings.dreaming_hour:
        return
    projects = await session.scalars(
        select(MemoryUnit.project_id)
        .where(
            MemoryUnit.scope == MemoryScope.PROJECT,
            MemoryUnit.unit_type == "notebook_note",
            MemoryUnit.status == MemoryUnitStatus.OBSERVATION,
        )
        .distinct()
    )
    units = MemoryUnitRepository(session)
    for project_id in projects:
        assert project_id is not None
        if not await project_idle(session, project_id):
            continue
        pending = await session.scalar(
            select(TransactionalOutbox.id)
            .where(
                TransactionalOutbox.destination == DESTINATION,
                TransactionalOutbox.payload["project_id"].astext == project_id,
                TransactionalOutbox.status.in_((DeliveryStatus.PENDING, DeliveryStatus.LEASED)),
            )
            .limit(1)
        )
        if pending is not None:
            continue
        notebook = await units.notebook(scope=MemoryScope.PROJECT, project_id=project_id)
        previous = await last_dream(session, project_id)
        fingerprint = notebook_fingerprint([note_view(note) for note in notebook.notes])
        if previous is not None and previous.payload.get("fingerprint") == fingerprint:
            continue
        goal_id = await session.scalar(
            select(ManagedGoal.id)
            .where(ManagedGoal.project_id == project_id)
            .order_by(ManagedGoal.created_at.desc(), ManagedGoal.id.desc())
            .limit(1)
        )
        if goal_id is None:
            continue
        date = local.date().isoformat()
        await session.execute(
            insert(TransactionalOutbox)
            .values(
                destination=DESTINATION,
                operation_type="dream_project",
                linked_entity_type="managed_goal",
                linked_entity_id=goal_id,
                idempotency_key=f"lead:dream:{project_id}:{date}",
                payload=DreamJob(project_id=project_id, local_date=date).model_dump(),
                status=DeliveryStatus.PENDING,
            )
            .on_conflict_do_nothing(constraint="uq_outbox_idempotency")
        )


async def dream_input(session: AsyncSession, project_id: str) -> dict[str, Any]:
    notebook = await MemoryUnitRepository(session).notebook(
        scope=MemoryScope.PROJECT, project_id=project_id
    )
    previous = await last_dream(session, project_id)
    # Retrospective jobs record terminal outcomes at their actual completion boundary.
    statement = (
        select(ManagedGoal.original_request, TransactionalOutbox.payload)
        .join(TransactionalOutbox, TransactionalOutbox.linked_entity_id == ManagedGoal.id)
        .where(
            ManagedGoal.project_id == project_id,
            TransactionalOutbox.destination == "goal_retrospective",
        )
    )
    if previous is not None:
        statement = statement.where(TransactionalOutbox.created_at > previous.updated_at)
    rows = await session.execute(
        statement.order_by(TransactionalOutbox.created_at.desc()).limit(24)
    )
    return {
        "project_id": project_id,
        "notes": [note_view(note) for note in notebook.notes],
        "finished_goals": [
            {"title": title[:200], "outcome": payload["outcome"]} for title, payload in rows
        ],
    }


async def apply_dream(
    session: AsyncSession,
    goal: ManagedGoal,
    job_id: uuid.UUID,
    response: DreamResponse,
    fingerprint: str,
) -> str:
    # Lock goal leases before the notebook, matching lead command lock order.
    if not await project_idle(session, goal.project_id, lock=True):
        raise ValueError("dream project has a leased lead turn")
    units = MemoryUnitRepository(session)
    await units.lock_notebook(MemoryScope.PROJECT, goal.project_id)
    notebook = await units.notebook(scope=MemoryScope.PROJECT, project_id=goal.project_id)
    notes = [note_view(note) for note in notebook.notes]
    if notebook_fingerprint(notes) != fingerprint:
        raise ValueError("dream notebook changed during inference")
    validate_dream(response, notes)
    by_id = {str(note.id): note for note in notebook.notes}
    for index, merge in enumerate(response.merge):
        note = await units.create_note(
            scope=MemoryScope.PROJECT,
            project_id=goal.project_id,
            goal_id=goal.id,
            text=merge.text,
            extraction_identity=f"lead-dream:{job_id}:{index}",
        )
        for replaced in merge.replaces:
            await units.supersede_note(by_id[replaced], superseded_by=note.id)
    for dropped in response.drop:
        await units.retract_note(by_id[dropped.id])
    if response.merge or response.drop:
        await enqueue_chat_notice(
            session,
            chat_id=goal.source_chat_id,
            thread_id=goal.source_thread_id,
            key=f"lead:dream-summary:{job_id}",
            text=(
                f"🌙 Разобрал заметки: объединил {sum(len(m.replaces) for m in response.merge)}, "
                f"убрал {len(response.drop)}"
            ),
        )
    result = await units.notebook(scope=MemoryScope.PROJECT, project_id=goal.project_id)
    return notebook_fingerprint([note_view(note) for note in result.notes])


class DreamConsumer:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        model: DreamModel,
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
            raise LeaseLost(f"dream lease lost: {token.item_id}")
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
                job = DreamJob.model_validate(item.payload)
                # This receipt survives operational replay, including empty model results.
                if job.applied:
                    await complete_outbox_item(session, token)
                    return True
                if item.attempt_count > self._max_attempts:
                    raise LeadModelUnavailable("dream_attempt_limit")
                goal = await session.get(ManagedGoal, item.linked_entity_id)
                if goal is None:
                    raise LeadModelUnavailable("dream_goal_missing")
                goal_id, project_id = goal.id, goal.project_id
                if goal.project_id != job.project_id:
                    raise LeadModelUnavailable("dream_project_mismatch")
                if not await project_idle(session, goal.project_id):
                    raise LeadModelUnavailable("dream_project_busy")
                context = await dream_input(session, goal.project_id)
                fingerprint = notebook_fingerprint(context["notes"])
                # Reuse the retrospective inference/CLI boundary without leasing the
                # provenance goal. Each outbox attempt has its own durable turn.
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
                    dream=True,
                    enabled=self._enabled,
                )
            binding = current_turn.set(authority)
            try:
                async with asyncio.timeout(self._timeout + 5):
                    response = await self._model.dream(
                        project_id,
                        context,
                        invocation_id=invocation_id,
                    )
                    # Validate even injected models before the first memory write.
                    response = DreamResponse.model_validate(response)
            finally:
                current_turn.reset(binding)
            async with self._factory.begin() as session:
                item = await self._locked_item(session, token)
                if not await invocation_current(session, authority):
                    raise LeaseLost(f"dream authority lost: {token.item_id}")
                goal = await session.get(ManagedGoal, goal_id)
                assert goal is not None
                result_fingerprint = await apply_dream(
                    session, goal, token.item_id, response, fingerprint
                )
                turn = await session.get(LeadershipTurn, invocation_id)
                assert turn is not None
                turn.status = "applied"
                turn.decision = response.model_dump()
                item.payload = job.model_copy(
                    update={"applied": True, "fingerprint": result_fingerprint}
                ).model_dump()
                await complete_outbox_item(session, token)
        except LeaseLost:
            _LOG.info("Dream lease lost: %s", token.item_id)
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
                        _LOG.warning("Dream %s dead-lettered: %s", token.item_id, category)
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
