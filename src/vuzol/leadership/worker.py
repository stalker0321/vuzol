"""One leased project-lead turn. The sequencer still owns the running package."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.batches import execute_batch
from vuzol.leadership.commands import CommandStatus, LeadCommand, payload_hash
from vuzol.leadership.evidence import assess_completion
from vuzol.leadership.model import (
    LeadDecision,
    LeadModel,
    LeadModelUnavailable,
    LeadSnapshot,
    decision_from_payload,
    decision_payload,
    normalize_usage,
)
from vuzol.leadership.persistence import (
    POLICY_REVISION,
    GoalDelegation,
    LeadershipTurn,
    ManagedGoal,
)
from vuzol.leadership.rules import delegation_is_expired
from vuzol.leadership.service import LeadershipCommandService
from vuzol.leadership.wakeups import completed_wakeup_key, enabled_wakeup_key, enqueue_goal_wakeup
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item, defer_outbox_item
from vuzol.storage.models import Event, TransactionalOutbox, WorkPackage
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

_TERMINAL_GOAL = frozenset({"paused", "cancelled", "completed"})


class ProjectLeadWorker:
    """Claim one wakeup, call the model outside the transaction, then apply."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        model: LeadModel,
        enabled: Callable[[], bool],
        lease_seconds: int = 120,
        repeat_limit: int = 2,
        branch_prefix: str = "vuzol-managed/",
        default_branches: Mapping[str, str] | None = None,
    ) -> None:
        self._factory = session_factory
        self._owner = owner
        self._model = model
        self._enabled = enabled
        self._lease_seconds = lease_seconds
        self._repeat_limit = repeat_limit
        self._branch_prefix = branch_prefix
        self._default_branches = dict(default_branches or {})

    async def process_one(self) -> bool:
        if not self._enabled():
            return False
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._lease_seconds,
                allowed_destinations=frozenset({"project_lead"}),
            )
        if token is None:
            async with self._factory.begin() as session:
                await self._reconcile(session)
            return False
        try:
            await self._handle(token)
        except Exception:
            await self._defer(token, "lead_turn_interrupted")
            raise
        return True

    async def _handle(self, token: OutboxLeaseToken) -> None:
        prepared = await self._prepare_turn(token)
        if prepared is None:
            return
        goal_id, wakeup_id, status = prepared
        if status == "claimed":
            snapshot = await self._snapshot(goal_id)
            try:
                decision = await self._model.decide(snapshot)
            except LeadModelUnavailable:
                await self._park_unconfigured(goal_id, wakeup_id, token)
                return
            await self._save_decision(goal_id, wakeup_id, decision)
            if not self._enabled():
                await self._park(goal_id, wakeup_id, token, reason="feature_disabled", bump=True)
                return
            if not await self._fence_ok(goal_id, wakeup_id):
                await self._park(goal_id, wakeup_id, token, reason=None, bump=False)
                return
        else:
            decision = await self._load_decision(goal_id, wakeup_id)
            if not self._enabled():
                await self._park(goal_id, wakeup_id, token, reason="feature_disabled", bump=True)
                return
            if not await self._response_admissible(goal_id):
                await self._park(goal_id, wakeup_id, token, reason=None, bump=False)
                return
        await self._apply_if_current(goal_id=goal_id, wakeup_id=wakeup_id, decision=decision)
        await self._ack(goal_id, wakeup_id, token)

    async def _prepare_turn(
        self, token: OutboxLeaseToken
    ) -> tuple[uuid.UUID, uuid.UUID, str] | None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            item = await uow.session.get(TransactionalOutbox, token.item_id)
            if item is None or item.destination != "project_lead":
                await self._complete(uow.session, token)
                return None
            goal_id = _goal_id(item)
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            if goal is None:
                await self._complete(uow.session, token)
                return None
            wakeup_id = item.id
            turn = await uow.session.scalar(
                select(LeadershipTurn).where(
                    LeadershipTurn.goal_id == goal.id,
                    LeadershipTurn.wakeup_id == wakeup_id,
                )
            )
            now = datetime.now(UTC)
            if turn is not None and turn.status in {"claimed", "responded"}:
                goal.lease_owner = self._owner
                goal.lease_until = now + timedelta(seconds=self._lease_seconds)
                return goal.id, wakeup_id, turn.status
            if turn is not None:
                await self._complete(uow.session, token)
                return None
            if (
                goal.lease_owner is not None
                and goal.lease_until is not None
                and goal.lease_until > now
            ):
                await self._defer_in_session(uow.session, token, "lead_lease_held")
                return None
            goal.lease_epoch += 1
            goal.lease_owner = self._owner
            goal.lease_until = now + timedelta(seconds=self._lease_seconds)
            uow.session.add(
                LeadershipTurn(
                    goal_id=goal.id,
                    wakeup_id=wakeup_id,
                    lease_epoch=goal.lease_epoch,
                    goal_version=goal.version,
                    scope_revision=goal.scope_revision,
                    fence_epoch=goal.fence_epoch,
                    profile_id="unconfigured",
                    usage={"cost_known": False},
                    status="claimed",
                    owner=self._owner,
                )
            )
            await uow.session.flush()
            return goal.id, wakeup_id, "claimed"

    async def _snapshot(self, goal_id: uuid.UUID) -> LeadSnapshot:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            if goal is None:
                raise LeadModelUnavailable("goal_missing")
            package = None
            if goal.current_package_id is not None:
                package = await uow.session.get(WorkPackage, goal.current_package_id)
            assessment = await assess_completion(uow.session, goal)
            status = None if package is None else package.status.value
            head = None if package is None else package.integration_head_commit
            criteria = tuple(item for item in goal.criteria if isinstance(item, dict))
            return LeadSnapshot(
                goal_id=goal.id,
                project_id=goal.project_id,
                lifecycle=goal.lifecycle,
                version=goal.version,
                fence_epoch=goal.fence_epoch,
                scope_revision=goal.scope_revision,
                criteria=criteria,
                satisfied_criteria=assessment.covered,
                integration_branch=goal.integration_branch,
                current_package_id=None if package is None else str(package.id),
                current_package_status=status,
                integration_head_commit=head,
                original_request=goal.original_request,
                scope_text=goal.scope_text,
                waiting_reason=goal.waiting_reason,
            )

    async def _save_decision(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, decision: LeadDecision
    ) -> None:
        body = decision_payload(decision)
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            turn = await _turn(uow.session, goal_id, wakeup_id)
            turn.status = "responded"
            turn.decision = body
            turn.profile_id = decision.profile_id
            turn.usage = normalize_usage(decision.usage)

    async def _load_decision(self, goal_id: uuid.UUID, wakeup_id: uuid.UUID) -> LeadDecision:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            turn = await _turn(uow.session, goal_id, wakeup_id)
            return decision_from_payload(turn.decision)

    async def _fence_ok(self, goal_id: uuid.UUID, wakeup_id: uuid.UUID) -> bool:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None:
                return False
            delegation = await uow.session.get(GoalDelegation, goal.delegation_id)
            if delegation is None:
                return False
            if goal.fence_epoch != turn.fence_epoch or goal.lease_epoch != turn.lease_epoch:
                return False
            if goal.lease_owner != self._owner:
                return False
            if goal.lifecycle in _TERMINAL_GOAL:
                return False
            if delegation.revoked_at is not None:
                return False
            if delegation_is_expired(delegation.expires_at, now=datetime.now(UTC)):
                return False
            return delegation.policy_revision == POLICY_REVISION

    async def _response_admissible(self, goal_id: uuid.UUID) -> bool:
        """A resumed response may repeat its own commands. A pause or revoke may not."""

        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            if goal is None or goal.lifecycle in _TERMINAL_GOAL:
                return False
            delegation = await uow.session.get(GoalDelegation, goal.delegation_id)
            if delegation is None or delegation.revoked_at is not None:
                return False
            return not delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))

    async def _apply_if_current(
        self, *, goal_id: uuid.UUID, wakeup_id: uuid.UUID, decision: LeadDecision
    ) -> None:
        if not decision.commands:
            await self._mark_waiting(goal_id, "model_idle")
            return
        version = await self._goal_version(goal_id)
        service = self._service()
        principal = await self._principal(goal_id)
        for draft in decision.commands:
            placeholder = LeadCommand(
                name=draft.name,
                project_id=await self._project_id(goal_id),
                goal_id=goal_id,
                expected_version=version,
                idempotency_key="pending",
                payload=dict(draft.payload),
                reason=draft.reason,
            )
            digest = payload_hash(placeholder)
            command = LeadCommand(
                name=draft.name,
                project_id=placeholder.project_id,
                goal_id=goal_id,
                expected_version=version,
                idempotency_key=f"lead:{goal_id}:{wakeup_id}:{draft.name}:{digest}",
                payload=dict(draft.payload),
                reason=draft.reason,
            )
            result = await service.apply_agent(principal, "worker", command)
            if result.version is not None:
                version = result.version
            if result.status in {
                CommandStatus.DENIED.value,
                CommandStatus.STALE.value,
                CommandStatus.UNSUPPORTED.value,
            }:
                return

    async def _ack(self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, token: OutboxLeaseToken) -> None:
        async with self._factory.begin() as session:
            await self._complete(session, token)
            turn = await _turn(session, goal_id, wakeup_id)
            goal = await session.get(ManagedGoal, goal_id)
            turn.status = "applied"
            if goal is not None and goal.lease_owner == self._owner:
                goal.lease_owner = None
                goal.lease_until = None

    async def _park_unconfigured(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, token: OutboxLeaseToken
    ) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is not None and goal.lifecycle not in {"cancelled", "completed"}:
                goal.lifecycle = "waiting"
                goal.waiting_reason = "lead_model_unconfigured"
                goal.lease_owner = None
                goal.lease_until = None
                goal.version += 1
                goal.fence_epoch += 1
                _system_event(uow.session, goal, "lead_model_unconfigured")
            turn.status = "lost"
            turn.usage = {"cost_known": False}
            turn.decision = None
            await self._complete(uow.session, token)

    async def _park(
        self,
        goal_id: uuid.UUID,
        wakeup_id: uuid.UUID,
        token: OutboxLeaseToken,
        *,
        reason: str | None,
        bump: bool,
    ) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            turn.status = "lost"
            if goal is not None and goal.lease_owner == self._owner:
                goal.lease_owner = None
                goal.lease_until = None
            if goal is not None and bump and reason is not None:
                if goal.lifecycle not in {"cancelled", "completed", "paused"}:
                    goal.lifecycle = "waiting"
                    goal.waiting_reason = reason
                goal.version += 1
                goal.fence_epoch += 1
                _system_event(uow.session, goal, reason)
            await self._complete(uow.session, token)

    async def _mark_waiting(self, goal_id: uuid.UUID, reason: str) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            if goal is None or goal.lifecycle in {"cancelled", "completed", "paused"}:
                return
            goal.lifecycle = "waiting"
            goal.waiting_reason = reason
            goal.lease_owner = None
            goal.lease_until = None
            goal.version += 1
            goal.fence_epoch += 1

    async def _complete(self, session: AsyncSession, token: OutboxLeaseToken) -> None:
        await complete_outbox_item(session, token)

    async def _defer(self, token: OutboxLeaseToken, reason: str) -> None:
        async with self._factory.begin() as session:
            await self._defer_in_session(session, token, reason)

    async def _defer_in_session(
        self, session: AsyncSession, token: OutboxLeaseToken, reason: str
    ) -> None:
        await defer_outbox_item(session, token, delay_seconds=0, reason=reason)

    async def _reconcile(self, session: AsyncSession) -> None:
        now = datetime.now(UTC)
        goals = (
            await session.scalars(
                select(ManagedGoal).where(ManagedGoal.lifecycle == "active").with_for_update()
            )
        ).all()
        for goal in goals:
            if (
                goal.lease_owner is not None
                and goal.lease_until is not None
                and goal.lease_until > now
            ):
                continue
            if goal.current_package_id is None:
                await enqueue_goal_wakeup(
                    session,
                    goal_id=goal.id,
                    idempotency_key=enabled_wakeup_key(goal.id),
                    package_id=None,
                    kind="enabled",
                )
                continue
            package = await session.get(WorkPackage, goal.current_package_id)
            if package is not None and package.status is WorkPackageStatus.COMPLETED:
                await enqueue_goal_wakeup(
                    session,
                    goal_id=goal.id,
                    idempotency_key=completed_wakeup_key(goal.id, package.id),
                    package_id=package.id,
                    kind="completed",
                )

    def _service(self) -> LeadershipCommandService:
        return LeadershipCommandService(
            self._factory,
            enabled=self._enabled(),
            branch_prefix=self._branch_prefix,
            repeat_limit=self._repeat_limit,
            default_branches=self._default_branches,
            batch_executor=execute_batch,
        )

    async def _goal_version(self, goal_id: uuid.UUID) -> int:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            if goal is None:
                raise LeadModelUnavailable("goal_missing")
            return goal.version

    async def _project_id(self, goal_id: uuid.UUID) -> str:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            if goal is None:
                raise LeadModelUnavailable("goal_missing")
            return goal.project_id

    async def _principal(self, goal_id: uuid.UUID) -> str:
        project_id = await self._project_id(goal_id)
        return server_agent_principal(project_id)


async def _turn(session: AsyncSession, goal_id: uuid.UUID, wakeup_id: uuid.UUID) -> LeadershipTurn:
    turn = await session.scalar(
        select(LeadershipTurn)
        .where(LeadershipTurn.goal_id == goal_id, LeadershipTurn.wakeup_id == wakeup_id)
        .with_for_update()
    )
    if turn is None:
        raise LeadModelUnavailable("turn_missing")
    return turn


def _goal_id(item: TransactionalOutbox) -> uuid.UUID:
    raw = item.payload.get("goal_id") if isinstance(item.payload, dict) else None
    if isinstance(raw, str):
        return uuid.UUID(raw)
    return item.linked_entity_id


def _system_event(session: AsyncSession, goal: ManagedGoal, reason: str) -> None:
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type="managed_goal.lead_parked",
            actor_type="system",
            actor_id="project-lead",
            previous_state=None,
            new_state=goal.lifecycle,
            payload={"reason": reason, "fence_epoch": goal.fence_epoch},
        )
    )
