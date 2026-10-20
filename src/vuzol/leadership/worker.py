"""One leased project-lead turn. The sequencer still owns the running package."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.batches import execute_batch
from vuzol.leadership.card import owner_answer_text
from vuzol.leadership.commands import CommandStatus, LeadCommand
from vuzol.leadership.continuation import continue_if_idle
from vuzol.leadership.evidence import assess_completion
from vuzol.leadership.feedback import result_context
from vuzol.leadership.fencing import TurnClaim, claim_current
from vuzol.leadership.invocation_context import TurnInvocationContext, current_turn
from vuzol.leadership.model import (
    LeadDecision,
    LeadModel,
    LeadModelUnavailable,
    LeadSnapshot,
    freeze_decision,
    normalize_usage,
)
from vuzol.leadership.persistence import (
    POLICY_REVISION,
    GoalDelegation,
    LeadershipCommandReceipt,
    LeadershipTurn,
    ManagedGoal,
)
from vuzol.leadership.rules import delegation_is_expired
from vuzol.leadership.service import LeadershipCommandService, request_owner_recovery
from vuzol.leadership.verification import GoalVerification
from vuzol.leadership.wakeups import (
    enabled_wakeup_key,
    enqueue_goal_wakeup,
    terminal_wakeup_key,
)
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item, defer_outbox_item
from vuzol.storage.models import Event, TransactionalOutbox, WorkPackage
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import WorkPackagePauseReason, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

_TERMINAL_GOAL = frozenset({"paused", "cancelled", "completed"})
_MODEL_BLOCKERS = frozenset(
    {
        "lead_model_unconfigured",
        "lead_profile_unselected",
        "lead_response_invalid",
        "lead_model_unavailable",
        "lead_budget_exceeded",
        "lead_context_limit",
        "goal_review_context_limit",
        "goal_review_invalid",
        "lead_inference_reconciliation",
    }
)


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
        verifier: GoalVerification | None = None,
    ) -> None:
        self._factory = session_factory
        self._owner = owner
        self._model = model
        self._enabled = enabled
        self._lease_seconds = lease_seconds
        self._repeat_limit = repeat_limit
        self._branch_prefix = branch_prefix
        self._default_branches = dict(default_branches or {})
        self._verifier = verifier

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
        except LeaseLost:
            return True
        except Exception:
            await self._release_outbox(token)
            raise
        return True

    async def _handle(self, token: OutboxLeaseToken) -> None:
        prepared = await self._prepare_turn(token)
        if prepared is None:
            return
        claim, status = prepared
        renewal = asyncio.create_task(self._renew_claim(claim))
        try:
            await self._run_turn(token, claim, status)
        finally:
            renewal.cancel()
            with suppress(asyncio.CancelledError):
                await renewal

    async def _run_turn(self, token: OutboxLeaseToken, claim: TurnClaim, status: str) -> None:
        goal_id, wakeup_id = claim.goal_id, claim.wakeup_id
        if status == "claimed":
            try:
                decision = await self._infer(claim)
            except LeadModelUnavailable as error:
                reason = str(error)
                if reason not in _MODEL_BLOCKERS:
                    reason = "lead_model_unavailable"
                await self._park_unconfigured(goal_id, wakeup_id, token, reason=reason, claim=claim)
                return
            saved = await self._save_decision(goal_id, wakeup_id, decision, claim=claim)
            if not saved:
                async with self._factory.begin() as session:
                    turn = await _turn(session, goal_id, wakeup_id)
                    if turn.status == "lost" and turn.lease_epoch == claim.lease_epoch:
                        await self._complete(session, token)
                    else:
                        await self._defer_in_session(session, token, "lead_lease_lost")
                return
        if not self._enabled():
            await self._park(
                goal_id, wakeup_id, token, reason="feature_disabled", bump=True, claim=claim
            )
            return
        if not await self._saved_decision_admissible(goal_id, wakeup_id, claim=claim):
            await self._park(goal_id, wakeup_id, token, reason=None, bump=False, claim=claim)
            return
        await self._apply_if_current(
            goal_id=goal_id, wakeup_id=wakeup_id, decision=LeadDecision(), claim=claim
        )
        await self._ack(goal_id, wakeup_id, token, claim=claim)

    async def _infer(self, claim: TurnClaim) -> LeadDecision:
        async with self._factory() as session:
            goal = await session.get(ManagedGoal, claim.goal_id)
            turn = await _turn(session, claim.goal_id, claim.wakeup_id)
            if goal is None:
                raise LeadModelUnavailable("goal_missing")
            context = TurnInvocationContext(
                claim=claim,
                turn_id=turn.id,
                project_id=goal.project_id,
                version=turn.goal_version,
                fence_epoch=turn.fence_epoch,
                scope_revision=turn.scope_revision,
                enabled=self._enabled,
            )
        token = current_turn.set(context)
        try:
            if self._verifier is not None:
                await self._verifier.verify(claim.goal_id, claim=claim)
            snapshot = await self._snapshot(claim.goal_id, wakeup_id=claim.wakeup_id)
            return await self._model.decide(snapshot)
        finally:
            current_turn.reset(token)

    async def _prepare_turn(self, token: OutboxLeaseToken) -> tuple[TurnClaim, str] | None:
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
            if goal.lifecycle in _TERMINAL_GOAL:
                if turn is not None and turn.status in {"claimed", "responded"}:
                    turn.status = "lost"
                await self._complete(uow.session, token)
                return None

            def prepared(status: str) -> tuple[TurnClaim, str]:
                return TurnClaim(
                    goal.id, wakeup_id, self._owner, goal.lease_epoch, token.generation
                ), status

            if turn is not None and turn.status in {"claimed", "responded"}:
                if (
                    turn.status == "claimed"
                    and turn.model_assignments is not None
                    and (
                        turn.goal_version != goal.version
                        or turn.scope_revision != goal.scope_revision
                        or turn.fence_epoch != goal.fence_epoch
                    )
                ):
                    turn.status = "lost"
                    await self._complete(uow.session, token)
                    return None
                if _lease_current(goal, turn, self._owner, now):
                    goal.lease_until = now + timedelta(seconds=self._lease_seconds)
                    return prepared(turn.status)
                held_by_other = (
                    goal.lease_owner not in {None, self._owner}
                    and goal.lease_until is not None
                    and goal.lease_until > now
                    and goal.lease_epoch == turn.lease_epoch
                )
                if held_by_other:
                    await self._defer_in_session(uow.session, token, "lead_lease_held")
                    return None
                goal.lease_epoch += 1
                goal.lease_owner = self._owner
                goal.lease_until = now + timedelta(seconds=self._lease_seconds)
                turn.lease_epoch = goal.lease_epoch
                turn.owner = self._owner
                if turn.status == "responded":
                    return prepared("responded")
                turn.status = "claimed"
                turn.decision = None
                turn.goal_version = goal.version
                turn.scope_revision = goal.scope_revision
                turn.fence_epoch = goal.fence_epoch
                turn.usage = {"cost_known": False}
                return prepared("claimed")
            if turn is not None:
                await self._complete(uow.session, token)
                return None
            if goal.lifecycle in _TERMINAL_GOAL or (
                goal.lifecycle == "waiting" and not await _waiting_exit_ready(uow.session, goal)
            ):
                await self._complete(uow.session, token)
                return None
            if goal.lifecycle == "waiting":
                goal.lifecycle = "active"
                goal.waiting_reason = None
                goal.wakeup_condition = None
                goal.version += 1
                goal.fence_epoch += 1
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
            return prepared("claimed")

    async def _snapshot(
        self, goal_id: uuid.UUID, *, wakeup_id: uuid.UUID | None = None
    ) -> LeadSnapshot:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            if goal is None:
                raise LeadModelUnavailable("goal_missing")
            package = None
            if goal.current_package_id is not None:
                package = await uow.session.get(WorkPackage, goal.current_package_id)
            assessment = await assess_completion(uow.session, goal, check_live_head=True)
            status = None if package is None else package.status.value
            head = None if package is None else package.integration_head_commit
            criteria = tuple(item for item in goal.criteria if isinstance(item, dict))
            completed = await uow.session.scalar(
                select(func.count())
                .select_from(WorkPackage)
                .where(
                    WorkPackage.managed_goal_id == goal.id,
                    WorkPackage.status == WorkPackageStatus.COMPLETED,
                )
            )
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
                completed_package_count=int(completed or 0),
                owner_answer=owner_answer_text(goal.constraints),
                invocation_id=wakeup_id,
                result_context=await result_context(uow.session, goal, package),
            )

    async def _save_decision(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, decision: LeadDecision, *, claim: TurnClaim
    ) -> bool:
        """Persist the response bound to the claim-time fence. A taken lease cannot save."""

        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None or turn.status != "claimed":
                return False
            if not await claim_current(uow.session, goal, claim, turn):
                # Controls may clear the goal lease. Never overwrite a replacement turn.
                if (
                    turn.lease_epoch == claim.lease_epoch
                    and turn.owner == claim.owner
                    and goal.fence_epoch != turn.fence_epoch
                ):
                    turn.status = "lost"
                return False
            turn.status = "responded"
            turn.profile_id = decision.profile_id
            turn.usage = normalize_usage(decision.usage)
            turn.decision = freeze_decision(
                decision,
                goal_id=goal.id,
                wakeup_id=wakeup_id,
                input_version=turn.goal_version,
                input_fence=turn.fence_epoch,
                input_scope=turn.scope_revision,
                input_lease_epoch=turn.lease_epoch,
            )
            return True

    async def _saved_decision_admissible(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, *, claim: TurnClaim
    ) -> bool:
        """Own receipts may replay. An owner control after the decision may not."""

        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None or not isinstance(turn.decision, dict):
                return False
            if not await claim_current(uow.session, goal, claim, turn):
                raise LeaseLost("project lead claim expired")
            delegation = await uow.session.get(GoalDelegation, goal.delegation_id)
            if delegation is None or delegation.revoked_at is not None:
                return False
            if delegation_is_expired(delegation.expires_at, now=datetime.now(UTC)):
                return False
            if delegation.policy_revision != POLICY_REVISION:
                return False
            if goal.lifecycle in _TERMINAL_GOAL:
                return False
            if goal.scope_revision != turn.scope_revision:
                return False
            body = turn.decision
            if body.get("input_fence") != turn.fence_epoch:
                return False
            if body.get("input_scope") != turn.scope_revision:
                return False
            commands = body.get("commands")
            if not isinstance(commands, list):
                return False
            if not commands:
                return goal.fence_epoch == turn.fence_epoch
            principal = server_agent_principal(goal.project_id)
            for ordinal, command in enumerate(commands):
                if not isinstance(command, dict):
                    return False
                receipt = await _command_receipt(
                    uow.session,
                    principal,
                    str(command.get("idempotency_key") or ""),
                )
                if receipt is None:
                    expected = command.get("expected_version")
                    return (
                        isinstance(expected, int)
                        and goal.version == expected
                        and goal.fence_epoch == turn.fence_epoch + ordinal
                    )
                if receipt.status not in {
                    CommandStatus.APPLIED.value,
                    CommandStatus.WAITING.value,
                }:
                    return False
            return True

    async def _apply_if_current(
        self, *, goal_id: uuid.UUID, wakeup_id: uuid.UUID, decision: LeadDecision, claim: TurnClaim
    ) -> None:
        del decision
        frozen = await self._frozen_commands(goal_id, wakeup_id)
        if frozen is None:
            return
        project_id, commands = frozen
        if not commands:
            await self._mark_waiting(goal_id, "model_idle", claim=claim)
            return
        service = self._service()
        principal = server_agent_principal(project_id)
        for command in commands:
            key = str(command["idempotency_key"])
            if await self._applied_receipt(principal, key):
                continue
            if not await self._saved_decision_admissible(goal_id, wakeup_id, claim=claim):
                await self._mark_lost(goal_id, wakeup_id, claim=claim)
                return
            payload = command.get("payload")
            expected_version = command.get("expected_version")
            if type(expected_version) is not int:
                await self._mark_lost(goal_id, wakeup_id, claim=claim)
                return
            result = await service.apply_agent(
                principal,
                "worker",
                LeadCommand(
                    name=str(command["name"]),
                    project_id=project_id,
                    goal_id=goal_id,
                    expected_version=expected_version,
                    idempotency_key=key,
                    payload=dict(payload) if isinstance(payload, dict) else {},
                    reason=str(command.get("reason") or ""),
                ),
                claim=claim,
            )
            if result.status in {
                CommandStatus.DENIED.value,
                CommandStatus.STALE.value,
                CommandStatus.UNSUPPORTED.value,
            }:
                await self._mark_waiting(goal_id, result.reason or "command_rejected", claim=claim)
                return
            if result.status == CommandStatus.WAITING.value or command["name"] in {
                "pause_goal",
                "cancel_goal",
                "propose_completion",
            }:
                return

    async def _ack(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, token: OutboxLeaseToken, *, claim: TurnClaim
    ) -> None:
        async with self._factory.begin() as session:
            # Match renewal/control lock order. A terminal command can release this
            # goal before ack; never release a successor's lease with the same name.
            goal = await session.get(ManagedGoal, goal_id, with_for_update=True)
            await self._complete(session, token)
            turn = await _turn(session, goal_id, wakeup_id)
            if turn.status != "lost":
                turn.status = "applied"
            if (
                goal is not None
                and goal.lease_owner == claim.owner
                and goal.lease_epoch == claim.lease_epoch
            ):
                await continue_if_idle(session, goal, repeat_limit=self._repeat_limit)
                goal.lease_owner = None
                goal.lease_until = None

    async def _park_unconfigured(
        self,
        goal_id: uuid.UUID,
        wakeup_id: uuid.UUID,
        token: OutboxLeaseToken,
        *,
        reason: str,
        claim: TurnClaim,
    ) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None or not await claim_current(uow.session, goal, claim, turn):
                if (
                    goal is not None
                    and turn.lease_epoch == claim.lease_epoch
                    and turn.owner == claim.owner
                    and goal.fence_epoch != turn.fence_epoch
                ):
                    turn.status = "lost"
                    await self._complete(uow.session, token)
                return
            if goal is not None and goal.lifecycle not in {"cancelled", "completed"}:
                goal.lifecycle = "waiting"
                goal.waiting_reason = reason
                goal.lease_owner = None
                goal.lease_until = None
                goal.version += 1
                goal.fence_epoch += 1
                _system_event(uow.session, goal, reason)
                await request_owner_recovery(uow.session, goal, reason)
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
        claim: TurnClaim,
    ) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None or not await claim_current(uow.session, goal, claim, turn):
                raise LeaseLost("project lead park claim expired")
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

    async def _mark_waiting(self, goal_id: uuid.UUID, reason: str, *, claim: TurnClaim) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            if goal is None or goal.lifecycle in {"cancelled", "completed", "paused"}:
                return
            if not await claim_current(uow.session, goal, claim):
                raise LeaseLost("project lead idle claim expired")
            goal.lifecycle = "waiting"
            goal.waiting_reason = reason
            goal.lease_owner = None
            goal.lease_until = None
            goal.version += 1
            goal.fence_epoch += 1
            await request_owner_recovery(uow.session, goal, reason)

    async def _renew_claim(self, claim: TurnClaim) -> None:
        while True:
            await asyncio.sleep(max(0.1, self._lease_seconds / 3))
            async with self._factory.begin() as session:
                goal = await session.get(ManagedGoal, claim.goal_id, with_for_update=True)
                if goal is None or not await claim_current(session, goal, claim):
                    return
                until = datetime.now(UTC) + timedelta(seconds=self._lease_seconds)
                goal.lease_until = until
                item = await session.get(TransactionalOutbox, claim.wakeup_id)
                assert item is not None
                item.lease_expires_at = until

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
                select(ManagedGoal)
                .where(ManagedGoal.lifecycle.in_(("active", "waiting")))
                .with_for_update()
            )
        ).all()
        for goal in goals:
            if (
                goal.lease_owner is not None
                and goal.lease_until is not None
                and goal.lease_until > now
            ):
                continue
            if goal.lifecycle == "waiting" and not await _waiting_exit_ready(session, goal):
                continue
            if goal.current_package_id is None:
                if goal.lifecycle != "active":
                    continue
                await enqueue_goal_wakeup(
                    session,
                    goal_id=goal.id,
                    idempotency_key=enabled_wakeup_key(goal.id),
                    package_id=None,
                    kind="enabled",
                )
                await continue_if_idle(session, goal, repeat_limit=self._repeat_limit)
                continue
            package = await session.get(WorkPackage, goal.current_package_id)
            kind = _terminal_kind(package) if package is not None else None
            if package is not None and kind is not None:
                await enqueue_goal_wakeup(
                    session,
                    goal_id=goal.id,
                    idempotency_key=terminal_wakeup_key(goal.id, package.id, package.version),
                    package_id=package.id,
                    kind=kind,
                )
            await continue_if_idle(session, goal, repeat_limit=self._repeat_limit)

    def _service(self) -> LeadershipCommandService:
        return LeadershipCommandService(
            self._factory,
            enabled=self._enabled(),
            branch_prefix=self._branch_prefix,
            repeat_limit=self._repeat_limit,
            default_branches=self._default_branches,
            batch_executor=execute_batch,
        )

    async def _release_outbox(self, token: OutboxLeaseToken) -> None:
        try:
            await self._defer(token, "lead_lease_lost")
        except LeaseLost:
            return

    async def _frozen_commands(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID
    ) -> tuple[str, list[dict[str, object]]] | None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None or not isinstance(turn.decision, dict):
                return None
            raw = turn.decision.get("commands")
            if not isinstance(raw, list):
                return None
            commands = [item for item in raw if isinstance(item, dict)]
            return goal.project_id, commands

    async def _applied_receipt(self, principal: str, key: str) -> bool:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            receipt = await _command_receipt(uow.session, principal, key)
            return receipt is not None and receipt.status in {
                CommandStatus.APPLIED.value,
                CommandStatus.WAITING.value,
            }

    async def _mark_lost(
        self, goal_id: uuid.UUID, wakeup_id: uuid.UUID, *, claim: TurnClaim
    ) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if goal is None or not await claim_current(uow.session, goal, claim, turn):
                raise LeaseLost("project lead lost claim expired")
            turn.status = "lost"
            if goal is not None and goal.lease_owner == self._owner:
                goal.lease_owner = None
                goal.lease_until = None


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


def _lease_current(goal: ManagedGoal, turn: LeadershipTurn, owner: str, now: datetime) -> bool:
    return (
        goal.lease_owner == owner
        and goal.lease_until is not None
        and goal.lease_until > now
        and goal.lease_epoch == turn.lease_epoch
    )


async def _command_receipt(
    session: AsyncSession, principal: str, key: str
) -> LeadershipCommandReceipt | None:
    if not key:
        return None
    return cast(
        LeadershipCommandReceipt | None,
        await session.scalar(
            select(LeadershipCommandReceipt).where(
                LeadershipCommandReceipt.actor_kind == "agent",
                LeadershipCommandReceipt.actor_subject == principal,
                LeadershipCommandReceipt.idempotency_key == key,
            )
        ),
    )


def _terminal_kind(package: WorkPackage) -> str | None:
    if package.status is WorkPackageStatus.COMPLETED:
        return "completed"
    if package.status in {WorkPackageStatus.STOPPED, WorkPackageStatus.DISCARDED}:
        return package.status.value
    if package.status is not WorkPackageStatus.PAUSED:
        return None
    if package.pause_reason is WorkPackagePauseReason.ITEM_FAILED:
        return "failed"
    if package.pause_reason is WorkPackagePauseReason.ITEM_BLOCKED:
        return "blocked"
    return None


async def _waiting_exit_ready(session: AsyncSession, goal: ManagedGoal) -> bool:
    condition = goal.wakeup_condition if isinstance(goal.wakeup_condition, dict) else {}
    if condition.get("kind") != "package":
        return False
    try:
        package_id = uuid.UUID(str(condition.get("package_id")))
    except (TypeError, ValueError):
        return False
    package = await session.get(WorkPackage, package_id)
    if package is None:
        return False
    return package.status in {
        WorkPackageStatus.COMPLETED,
        WorkPackageStatus.PAUSED,
        WorkPackageStatus.STOPPED,
        WorkPackageStatus.DISCARDED,
    }


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
