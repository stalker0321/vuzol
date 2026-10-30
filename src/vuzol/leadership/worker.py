"""One leased project-lead turn with explicit task dispatch and acceptance."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from sqlalchemy import and_, func, literal, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.amendments import apply_pending_amendment
from vuzol.leadership.batches import execute_batch
from vuzol.leadership.card import owner_answer_text
from vuzol.leadership.commands import CommandStatus, LeadCommand
from vuzol.leadership.continuation import continue_if_idle
from vuzol.leadership.evidence import assess_completion
from vuzol.leadership.feedback import result_context
from vuzol.leadership.fencing import TurnClaim, claim_current
from vuzol.leadership.invocation_context import TurnInvocationContext, current_turn
from vuzol.leadership.job_runner import ResearchJobRunner
from vuzol.leadership.limits import parse_work_limits
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
    current_request,
    superseded_request,
)
from vuzol.leadership.planning import PLAN_EVENTS, publish_outcome
from vuzol.leadership.rules import delegation_is_expired
from vuzol.leadership.service import LeadershipCommandService, request_owner_recovery
from vuzol.leadership.verification import GoalVerification
from vuzol.leadership.wakeups import (
    enabled_wakeup_key,
    enqueue_goal_wakeup,
)
from vuzol.providers.budgets import goal_cost_totals
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_outbox_item, complete_outbox_item, defer_outbox_item
from vuzol.storage.models import Event, TransactionalOutbox, WorkPackage
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import DeliveryStatus, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork

# A refusal meaning "too early": the package answers it, not the owner.
_NOT_YET_REASONS = frozenset({"result_not_ready", "project_task_running"})
_RETRIED_ONCE = frozenset({"lead_response_invalid", "goal_review_invalid"})
_INFRASTRUCTURE_FAILURES = frozenset({"provider_unavailable", "rate_limited", "timeout"})
_TERMINAL_GOAL = frozenset({"cancelled", "completed"})
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
        "goal_review_unavailable",
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
        model_retry_delays_seconds: tuple[float, ...] = (30, 120, 600),
        repeat_limit: int = 2,
        branch_prefix: str = "vuzol-managed/",
        default_goal_max_cost: float | None = None,
        default_branches: Mapping[str, str] | None = None,
        repository_paths: Mapping[str, Path] | None = None,
        verifier: GoalVerification | None = None,
        job_runner: ResearchJobRunner | None = None,
    ) -> None:
        self._factory = session_factory
        self._owner = owner
        self._model = model
        self._enabled = enabled
        self._lease_seconds = lease_seconds
        self._model_retry_delays_seconds = model_retry_delays_seconds[:3]
        self._repeat_limit = repeat_limit
        self._branch_prefix = branch_prefix
        self._default_goal_max_cost = default_goal_max_cost
        self._default_branches = dict(default_branches or {})
        self._repository_paths = dict(repository_paths or {})
        self._verifier = verifier
        self.job_runner = job_runner

    async def aclose(self) -> None:
        if self.job_runner is not None:
            await self.job_runner.aclose()

    async def process_one(self) -> bool:
        if self.job_runner is not None:
            self.job_runner.start()
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
                if error.failure_category is not None and not error.definitive:
                    reason = "lead_inference_reconciliation"
                await self._park_unconfigured(
                    goal_id,
                    wakeup_id,
                    token,
                    reason=reason,
                    claim=claim,
                    retryable=error.retryable,
                    definitive=error.definitive,
                    failure_category=error.failure_category,
                )
                return
            saved = await self._save_decision(goal_id, wakeup_id, decision, claim=claim)
            if not saved:
                await self._planner_lost(claim)
                async with self._factory.begin() as session:
                    turn = await _turn(session, goal_id, wakeup_id)
                    goal = await session.get(ManagedGoal, goal_id)
                    if await self._defer_terminal_owner_input(session, goal, turn, token):
                        return
                    elif turn.status == "lost" and turn.lease_epoch == claim.lease_epoch:
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
            await self._planner_lost(claim)
            await self._park(goal_id, wakeup_id, token, reason=None, bump=False, claim=claim)
            return
        await self._apply_if_current(
            goal_id=goal_id, wakeup_id=wakeup_id, decision=LeadDecision(), claim=claim
        )
        await self._ack(goal_id, wakeup_id, token, claim=claim)

    async def _planner_lost(self, claim: TurnClaim) -> None:
        async with self._factory.begin() as session:
            item = await session.get(TransactionalOutbox, claim.wakeup_id)
            goal = await session.get(ManagedGoal, claim.goal_id, with_for_update=True)
            if (
                item
                and goal
                and item.payload.get("wakeup_kind") == "plan_requested"
                and goal.lifecycle not in _TERMINAL_GOAL
            ):
                await publish_outcome(session, goal, item.id, reason="planner_scope_changed")

    async def _infer(self, claim: TurnClaim) -> LeadDecision:
        async with self._factory() as session:
            goal = await session.get(ManagedGoal, claim.goal_id)
            turn = await _turn(session, claim.goal_id, claim.wakeup_id)
            if goal is None:
                raise LeadModelUnavailable("goal_missing")
            item = await session.get(TransactionalOutbox, claim.wakeup_id)
            conversational = item is not None and item.payload.get("wakeup_kind") in {
                "owner_message",
                "owner_routing_answer",
                "job_finished",
                *PLAN_EVENTS,
                "planning_requested",
            }
            context = TurnInvocationContext(
                claim=claim,
                turn_id=turn.id,
                project_id=goal.project_id,
                version=turn.goal_version,
                fence_epoch=turn.fence_epoch,
                scope_revision=turn.scope_revision,
                enabled=self._enabled,
                conversational=conversational,
            )
        token = current_turn.set(context)
        try:
            review_failure = None
            if self._verifier is not None and not conversational:
                try:
                    await self._verifier.verify(claim.goal_id, claim=claim)
                except LeadModelUnavailable as error:
                    if str(error) != "goal_budget_exhausted":
                        raise
                    review_failure = str(error)
            snapshot = await self._snapshot(claim.goal_id, wakeup_id=claim.wakeup_id)
            if review_failure is not None:
                snapshot = replace(
                    snapshot,
                    result_context={
                        **snapshot.result_context,
                        "goal_review_failure": review_failure,
                    },
                )
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
            from vuzol.leadership.fence import lock_project_admission

            project_id = await uow.session.scalar(
                select(ManagedGoal.project_id).where(ManagedGoal.id == goal_id)
            )
            if project_id is not None:
                await lock_project_admission(uow.session, project_id)
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            if goal is None:
                await self._complete(uow.session, token)
                return None
            owner_event = item.payload.get("wakeup_kind") == "owner_message"
            job_event = item.payload.get("wakeup_kind") == "job_finished"
            if job_event and goal.lifecycle == "paused":
                await self._complete(uow.session, token)
                return None
            if owner_event and goal.lifecycle in _TERMINAL_GOAL:
                restart_source = item.payload.get("restart_from_event_id")
                if (
                    goal.constraints.get("restart_handoff", {}).get("source_event_id")
                    == str(item.id)
                ) or (
                    restart_source is not None
                    and goal.constraints.get("restart_from_event_id") == restart_source
                ):
                    # Neither side of a committed handoff may admit the same request again.
                    await self._complete(uow.session, token)
                    return None
                previous_turn = await uow.session.scalar(
                    select(LeadershipTurn).where(
                        LeadershipTurn.goal_id == goal.id,
                        LeadershipTurn.wakeup_id == item.id,
                    )
                )
                # A saved response already consumed the message (including crash-before-ack).
                # Unprocessed owner input belongs to a new/live conversation; task results do not.
                if previous_turn is None or previous_turn.decision is None:
                    from vuzol.leadership.events import live_or_draft
                    from vuzol.storage.models import ProjectDiscussionSession

                    discussion = await uow.session.get(
                        ProjectDiscussionSession, uuid.UUID(item.payload["session_id"])
                    )
                    assert discussion is not None
                    old_id = goal.id
                    goal = await live_or_draft(
                        uow.session,
                        project_id=goal.project_id,
                        owner_user_id=goal.owner_user_id,
                        discussion=discussion,
                        text=item.payload["text"],
                        scope=item.payload.get("request_scope", item.payload["text"]),
                        criteria=item.payload.get("request_criteria", []),
                        constraints={
                            "work_limits": item.payload.get("request_work_limits", {}),
                            "request_origin": item.payload.get("request_origin"),
                            "project_default_branch": goal.constraints.get(
                                "project_default_branch", "main"
                            ),
                        },
                        branch_prefix=self._branch_prefix,
                        default_goal_max_cost=self._default_goal_max_cost,
                    )
                    item.linked_entity_id = goal.id
                    item.payload = {
                        **item.payload,
                        "previous_goal_id": str(old_id),
                        "goal_id": str(goal.id),
                        "goal_version": goal.version,
                        "scope_revision": goal.scope_revision,
                        "delegation_id": str(goal.delegation_id),
                    }
                    if previous_turn is not None:
                        previous_turn.status = "superseded"
            planning_event = item.payload.get("wakeup_kind") in {*PLAN_EVENTS, "planning_requested"}
            if not owner_event and item.payload.get("task_id"):
                from vuzol.storage.models import Step, Task
                from vuzol.storage.types import TaskStatus

                task = await uow.session.get(Task, uuid.UUID(item.payload["task_id"]))
                stale = task is None or item.payload.get("package_id") != str(
                    goal.current_package_id
                )
                if item.payload.get("step_id"):
                    step = await uow.session.get(Step, uuid.UUID(item.payload["step_id"]))
                    stale = (
                        stale
                        or step is None
                        or step.lease_generation != item.payload.get("generation")
                        or step.version != item.payload.get("step_version")
                    )
                    stale = stale or (
                        task is not None
                        and task.status
                        not in {TaskStatus.EXECUTING, TaskStatus.REVIEWING, TaskStatus.VALIDATING}
                    )
                else:
                    stale = stale or (
                        task is not None and task.version != item.payload.get("generation")
                    )
                if stale:
                    await self._complete(uow.session, token)
                    return None
            # Intake order also includes voices still being transcribed. Telegram
            # message ids are only ordered within a chat, not across the project.
            received_at = (
                item.payload["received_at"]
                if owner_event
                else item.created_at.astimezone(UTC).isoformat(timespec="microseconds")
            )
            source_id = item.payload["intake_id"] if owner_event else str(item.id)
            earlier = await uow.session.scalar(
                select(TransactionalOutbox.id)
                .where(
                    or_(
                        and_(
                            TransactionalOutbox.destination == "project_lead",
                            TransactionalOutbox.linked_entity_id == goal.id,
                            TransactionalOutbox.payload["wakeup_kind"].as_string()
                            == "owner_message",
                        ),
                        and_(
                            TransactionalOutbox.destination == "telegram_file",
                            TransactionalOutbox.payload["lead_goal_id"].as_string() == str(goal.id),
                        ),
                    ),
                    TransactionalOutbox.status.in_(
                        (DeliveryStatus.PENDING, DeliveryStatus.LEASED, DeliveryStatus.AMBIGUOUS)
                    ),
                    tuple_(
                        TransactionalOutbox.payload["received_at"].as_string(),
                        TransactionalOutbox.payload["intake_id"].as_string(),
                    )
                    < tuple_(literal(received_at), literal(source_id)),
                )
                .limit(1)
            )
            if earlier is not None:
                await self._defer_in_session(uow.session, token, "lead_event_order")
                return None
            from vuzol.leadership.legacy_plans import convert_legacy_plan

            await convert_legacy_plan(uow, goal, self._repository_paths.get(goal.project_id))
            wakeup_id = item.id
            turn = await uow.session.scalar(
                select(LeadershipTurn).where(
                    LeadershipTurn.goal_id == goal.id,
                    LeadershipTurn.wakeup_id == wakeup_id,
                )
            )
            # A control can supersede an unclaimed outcome. Once a turn has a
            # decision, its own receipts may have advanced the goal version;
            # preserve the normal receipt/fence replay path in that case.
            if (
                turn is None
                and item.payload.get("wakeup_kind") in {"plan_ready", "plan_failed"}
                and item.payload.get("goal_version") != goal.version
            ):
                await self._complete(uow.session, token)
                return None
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
                    if item.payload.get("wakeup_kind") == "plan_requested":
                        await publish_outcome(
                            uow.session, goal, item.id, reason="planner_scope_changed"
                        )
                    await self._complete(uow.session, token)
                    return None
                if _lease_current(goal, turn, self._owner, now):
                    goal.lease_until = now + timedelta(seconds=self._lease_seconds)
                    return prepared(turn.status)
                held_by_other = (
                    goal.lease_owner is not None
                    and goal.lease_until is not None
                    and goal.lease_until > now
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
            if (
                goal.lease_owner is not None
                and goal.lease_until is not None
                and goal.lease_until > now
            ):
                await self._defer_in_session(uow.session, token, "lead_lease_held")
                return None
            if not owner_event and item.payload.get("task_id"):
                from vuzol.discussion.sequencer import WorkPackageSequencer
                from vuzol.storage.models import Task
                from vuzol.workflows.lead_events import OBSERVED_TASK_STATUSES

                task = await uow.session.get(Task, uuid.UUID(item.payload["task_id"]))
                if (
                    task is not None
                    and task.status in OBSERVED_TASK_STATUSES
                    and task.status.value != "waiting_approval"
                ):
                    await WorkPackageSequencer(uow).observe_terminal(task_id=task.id)
            await apply_pending_amendment(uow.session, goal)
            if goal.constraints.get("pending_amendment") and not owner_event and not job_event:
                if item.payload.get("wakeup_kind") == "plan_requested":
                    await publish_outcome(uow.session, goal, item.id, reason="amendment_pending")
                await self._complete(uow.session, token)
                return None
            if (
                not owner_event
                and not planning_event
                and not job_event
                and item.payload.get("wakeup_kind") != "owner_routing_answer"
                and (
                    goal.lifecycle == "paused"
                    or (
                        goal.lifecycle == "waiting"
                        and not await _waiting_exit_ready(uow.session, goal)
                    )
                )
            ):
                await self._complete(uow.session, token)
                return None
            if (
                goal.lifecycle == "waiting"
                and not owner_event
                and not planning_event
                and not job_event
            ):
                from vuzol.leadership.persistence import has_owner_mandate

                goal.lifecycle = "active" if has_owner_mandate(goal) else "draft"
                goal.waiting_reason = None
                from vuzol.telegram.attention import sync_goal_attention

                await sync_goal_attention(uow.session, goal)
                goal.wakeup_condition = None
                goal.version += 1
                goal.fence_epoch += 1
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
            from vuzol.discussion.domain import DomainError
            from vuzol.execution.git import GitError
            from vuzol.leadership.jobs import list_jobs
            from vuzol.leadership.planning import source_commit

            job_head = None
            with suppress(DomainError, GitError, OSError):
                job_head = await source_commit(self._repository_paths.get(goal.project_id), goal)
            criteria = tuple(item for item in goal.criteria if isinstance(item, dict))
            completed = await uow.session.scalar(
                select(func.count())
                .select_from(WorkPackage)
                .where(
                    WorkPackage.managed_goal_id == goal.id,
                    WorkPackage.status == WorkPackageStatus.COMPLETED,
                )
            )
            item = await uow.session.get(TransactionalOutbox, wakeup_id) if wakeup_id else None
            owner_event = (
                dict(item.payload)
                if item is not None and item.payload.get("wakeup_kind") == "owner_message"
                else None
            )
            from vuzol.leadership.tasks import task_context
            from vuzol.storage.models import ConversationTurn

            history = []
            if owner_event is not None:
                source_turn = await uow.session.get(
                    ConversationTurn, uuid.UUID(owner_event["source_turn_id"])
                )
                if source_turn is not None:
                    prior_turns = list(
                        await uow.session.scalars(
                            select(ConversationTurn)
                            .where(
                                ConversationTurn.session_id == source_turn.session_id,
                                ConversationTurn.ordinal < source_turn.ordinal,
                            )
                            .order_by(ConversationTurn.ordinal.desc())
                            .limit(8)
                        )
                    )
                    history = [
                        {"role": t.role.value, "text": t.content[:2000]}
                        for t in reversed(prior_turns)
                    ]

            from vuzol.leadership.notebooks import note_view
            from vuzol.storage.types import MemoryScope

            project_notebook = await uow.memory_units.notebook(
                scope=MemoryScope.PROJECT, project_id=goal.project_id
            )
            global_notebook = await uow.memory_units.notebook(scope=MemoryScope.GLOBAL)
            spent, reserved = await goal_cost_totals(uow.session, goal.id)
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
                request=current_request(goal),
                superseded_request=superseded_request(goal),
                scope_text=goal.scope_text,
                waiting_reason=goal.waiting_reason,
                completed_package_count=int(completed or 0),
                owner_answer=owner_answer_text(goal.constraints),
                owner_event=owner_event,
                invocation_id=wakeup_id,
                project_notebook=tuple(note_view(note) for note in project_notebook.notes),
                global_notebook=tuple(note_view(note) for note in global_notebook.notes),
                budget={
                    "spent": float(spent),
                    "reserved": float(reserved),
                    "cap": parse_work_limits(goal.constraints.get("work_limits", {})).max_cost,
                },
                result_context={
                    **await result_context(uow.session, goal, package),
                    "owner_answer": _current_answer(goal.constraints),
                    "work_limits": goal.constraints.get("work_limits", {}),
                    "conversation": history,
                    "task_control": await task_context(uow.session, goal),
                    "jobs": await list_jobs(uow.session, goal_id=goal.id, current_head=job_head),
                    "event": dict(item.payload) if item is not None else None,
                    "pending_amendment": goal.constraints.get("pending_amendment"),
                    "queued_amendment": goal.constraints.get("queued_amendment"),
                },
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
            if (
                not await claim_current(uow.session, goal, claim, turn)
                or goal.version != turn.goal_version
                or goal.fence_epoch != turn.fence_epoch
                or goal.scope_revision != turn.scope_revision
            ):
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
            from vuzol.leadership.text import is_plan_approval_question

            item = await uow.session.get(TransactionalOutbox, wakeup_id)
            if (
                item is not None
                and item.payload.get("wakeup_kind") == "owner_message"
                and decision.reply.strip()
                and not any(
                    command.name in {"ask_owner", "request_permission"}
                    for command in decision.commands
                )
                and not (
                    goal.waiting_reason == "plan_approval"
                    and is_plan_approval_question(decision.reply)
                )
            ):
                from vuzol.discussion.memory_service import DiscussionMemoryService
                from vuzol.leadership.text import owner_text
                from vuzol.storage.types import (
                    ConversationTurnRole,
                    ConversationTurnSource,
                    InteractionMode,
                )

                reply_session_id = uuid.UUID(item.payload["session_id"])
                reply_id, _ = await DiscussionMemoryService(uow).append_turn(
                    session_id=reply_session_id,
                    role=ConversationTurnRole.ASSISTANT,
                    source=ConversationTurnSource.MODEL,
                    content=owner_text(decision.reply),
                    classifier_mode=InteractionMode.DISCUSSION,
                )
                await uow.outbox.enqueue(
                    destination="telegram",
                    operation_type="send_message",
                    entity_type="conversation_turn",
                    entity_id=reply_id,
                    idempotency_key=f"lead:reply:{wakeup_id}",
                    payload={
                        "role": "discussion_reply",
                        "session_id": str(reply_session_id),
                        "source_turn_id": item.payload["source_turn_id"],
                    },
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
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, wakeup_id)
            owner_event = item is not None and item.payload.get("wakeup_kind") == "owner_message"
            keep_wait = False
            if (
                not commands
                and item is not None
                and item.payload.get("wakeup_kind") == "job_finished"
            ):
                goal = await session.get(ManagedGoal, goal_id)
                keep_wait = goal is not None and goal.lifecycle == "waiting"
        if not commands:
            if owner_event or keep_wait:
                return
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
                if not owner_event:
                    await self._mark_waiting(
                        goal_id, result.reason or "command_rejected", claim=claim
                    )
                return
            if result.status == CommandStatus.WAITING.value or command["name"] in {
                "pause_goal",
                "amend_goal",
                "request_revision",
                "cancel_goal",
                "propose_completion",
                "request_plan",
                "revise_plan",
                "propose_batch",
                "propose_plan",
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
                if (
                    turn.status == "applied"
                    and goal.lifecycle == "waiting"
                    and goal.waiting_reason in _MODEL_BLOCKERS
                ):
                    # The model just answered, so whatever parked the goal on it is gone;
                    # an owner message alone never leaves waiting (live 2026-10-27).
                    from vuzol.leadership.persistence import has_owner_mandate
                    from vuzol.telegram.attention import sync_goal_attention

                    goal.lifecycle = "active" if has_owner_mandate(goal) else "draft"
                    goal.waiting_reason = None
                    goal.wakeup_condition = None
                    await sync_goal_attention(session, goal)
                    goal.version += 1
                    goal.fence_epoch += 1
                item = await session.get(TransactionalOutbox, wakeup_id)
                if item is None or item.payload.get("wakeup_kind") != "owner_message":
                    await continue_if_idle(
                        session,
                        goal,
                        repeat_limit=self._repeat_limit,
                        count_pending=item is not None
                        and item.payload.get("wakeup_kind") == "plan_requested",
                    )
                goal.lease_owner = None
                goal.lease_until = None

    async def _defer_terminal_owner_input(
        self,
        session: AsyncSession,
        goal: ManagedGoal | None,
        turn: LeadershipTurn,
        token: OutboxLeaseToken,
    ) -> bool:
        if goal is None or goal.lifecycle not in _TERMINAL_GOAL or turn.decision is not None:
            return False
        item = await session.get(TransactionalOutbox, token.item_id)
        if item is None or item.payload.get("wakeup_kind") != "owner_message":
            return False
        # Completion can fence either the provider or response persistence. In
        # both cases the unanswered input must reach the next live conversation.
        turn.status = "lost"
        await self._defer_in_session(session, token, "lead_goal_finished")
        return True

    async def _park_unconfigured(
        self,
        goal_id: uuid.UUID,
        wakeup_id: uuid.UUID,
        token: OutboxLeaseToken,
        *,
        reason: str,
        claim: TurnClaim,
        retryable: bool = False,
        definitive: bool = False,
        failure_category: str | None = None,
    ) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            turn = await _turn(uow.session, goal_id, wakeup_id)
            if await self._defer_terminal_owner_input(uow.session, goal, turn, token):
                return
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
            if goal.fence_epoch != turn.fence_epoch:
                # The goal moved on mid-call (the owner approved the plan) and the guard
                # cancelled the stale call: not a model failure (live pilot 2026-10-24).
                # Like a restart that finds a stale claimed turn: the newer event drives.
                turn.status = "lost"
                goal.lease_owner = None
                goal.lease_until = None
                await self._complete(uow.session, token)
                return
            item = await uow.session.get(TransactionalOutbox, wakeup_id)
            retry_count = int(item.payload.get("model_retry_count", 0)) if item else 0
            delays = self._model_retry_delays_seconds
            infrastructure_retry = (
                definitive
                and failure_category in _INFRASTRUCTURE_FAILURES
                and retry_count < len(delays)
            )
            invalid_retry = (
                reason in _RETRIED_ONCE
                and item is not None
                and "retry_of" not in item.payload
                and (failure_category is None or definitive)
            )
            if (
                item is not None
                and (infrastructure_retry or invalid_retry)
                and retryable
                and goal.lifecycle not in {"cancelled", "completed", "paused"}
            ):
                # A fresh wakeup gives the same event a fresh invocation identity.
                # Keep it durable and unclaimable until its backoff has elapsed.
                delay = delays[retry_count] if infrastructure_retry else 0
                uow.session.add(
                    TransactionalOutbox(
                        destination="project_lead",
                        operation_type="lead_wakeup",
                        linked_entity_type="managed_goal",
                        linked_entity_id=goal.id,
                        idempotency_key=f"lead:retry-model:{wakeup_id}",
                        payload={
                            **item.payload,
                            "retry_of": str(wakeup_id),
                            "model_retry_count": retry_count + 1,
                        },
                        status=DeliveryStatus.PENDING,
                        available_at=datetime.now(UTC) + timedelta(seconds=delay),
                    )
                )
                turn.status = "lost"
                turn.usage = {"cost_known": False}
                turn.decision = None
                goal.lease_owner = None
                goal.lease_until = None
                await self._complete(uow.session, token)
                return
            if goal.lifecycle not in {"cancelled", "completed"}:
                if goal.lifecycle != "paused":
                    goal.lifecycle = "waiting"
                    goal.waiting_reason = reason
                goal.lease_owner = None
                goal.lease_until = None
                goal.version += 1
                goal.fence_epoch += 1
                _system_event(uow.session, goal, reason)
                await request_owner_recovery(
                    uow.session,
                    goal,
                    reason,
                    failure_category=failure_category,
                    retries=retry_count,
                )
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
            if isinstance(turn.decision, dict):
                from vuzol.leadership.notices import reject_saved_commands

                await reject_saved_commands(
                    uow.session, goal, wakeup_id, turn.decision, "decision_stale"
                )
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
                from vuzol.leadership.service import enqueue_goal_status

                await enqueue_goal_status(uow.session, goal)
            await self._complete(uow.session, token)

    async def _mark_waiting(self, goal_id: uuid.UUID, reason: str, *, claim: TurnClaim) -> None:
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            if goal is None or goal.lifecycle in {"cancelled", "completed", "paused"}:
                return
            if not await claim_current(uow.session, goal, claim):
                raise LeaseLost("project lead idle claim expired")
            from vuzol.telegram.attention import pending_owner_plan

            # Live 2026-10-30: a denied command replaced the plan-approval wait with
            # "уточни следующий шаг", hiding the plan that only needed the button.
            pending = await pending_owner_plan(uow.session, goal)
            goal.lifecycle = "waiting"
            goal.lease_owner = None
            goal.lease_until = None
            goal.version += 1
            goal.fence_epoch += 1
            if pending is not None:
                # An idle lead with a plan on the owner's card is waiting for that
                # decision, not stuck: no recovery prompt for the owner.
                goal.waiting_reason = "plan_approval"
                goal.wakeup_condition = {"kind": "plan_approval", "revision_id": pending[1]}
                if reason != "model_idle":
                    # Bring the card with the approve button back to the bottom.
                    uow.session.add(
                        TransactionalOutbox(
                            destination="work_package_projection",
                            operation_type="repost_plan",
                            linked_entity_type="work_package",
                            linked_entity_id=pending[0].id,
                            idempotency_key=f"wp:repost:pending:{goal.id}:{goal.version}",
                            payload={"package_id": str(pending[0].id)},
                            status=DeliveryStatus.PENDING,
                        )
                    )
                return
            if reason in _NOT_YET_REASONS and goal.current_package_id is not None:
                # Live pilot 2026-10-25: the lead accepted a result a minute early and
                # the refusal asked the owner "уточни следующий шаг". "Not yet" is the
                # package's to answer: wait for its next decision.
                from vuzol.leadership.wakeups import enqueue_goal_wakeup

                package = await uow.session.get(WorkPackage, goal.current_package_id)
                if package is not None and package.status is not WorkPackageStatus.DISCARDED:
                    condition = {"kind": "package", "package_id": str(package.id)}
                    goal.waiting_reason = "wait_for"
                    goal.wakeup_condition = condition
                    if await _waiting_exit_ready(uow.session, goal):
                        # Ready already: one wakeup per package state, so a repeated
                        # early command cannot loop.
                        await enqueue_goal_wakeup(
                            uow.session,
                            goal_id=goal.id,
                            idempotency_key=f"lead:not-yet:{package.id}:{package.version}",
                            package_id=package.id,
                            kind="wait_ready",
                        )
                    return
            goal.waiting_reason = reason
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
        from vuzol.telegram.attention import sync_goal_attention

        now = datetime.now(UTC)
        goals = (
            await session.scalars(
                select(ManagedGoal)
                .where(ManagedGoal.lifecycle.in_(("draft", "active", "waiting", "paused")))
                .with_for_update()
            )
        ).all()
        for goal in goals:
            await sync_goal_attention(session, goal)
            if (
                goal.lease_owner is not None
                and goal.lease_until is not None
                and goal.lease_until > now
            ):
                continue
            if goal.constraints.get("pending_amendment") or goal.constraints.get(
                "queued_amendment"
            ):
                if await apply_pending_amendment(session, goal):
                    await enqueue_goal_wakeup(
                        session,
                        goal_id=goal.id,
                        package_id=goal.current_package_id,
                        idempotency_key=f"lead:amended:{goal.id}:{goal.scope_revision}",
                        kind="amended",
                    )
                if goal.constraints.get("pending_amendment"):
                    continue
            if goal.lifecycle == "paused":
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
            from vuzol.storage.models import MaterializationLink, Task
            from vuzol.workflows.lead_events import publish_task_event

            tasks = await session.scalars(
                select(Task)
                .join(
                    MaterializationLink,
                    MaterializationLink.task_id == Task.id,
                )
                .where(MaterializationLink.work_package_id == goal.current_package_id)
            )
            for task in tasks:
                # Same keys as the state transaction: recovery cannot create a
                # second ordinary notification for an already consumed event.
                await publish_task_event(session, task)
            await continue_if_idle(session, goal, repeat_limit=self._repeat_limit)

    def _service(self) -> LeadershipCommandService:
        return LeadershipCommandService(
            self._factory,
            enabled=self._enabled(),
            branch_prefix=self._branch_prefix,
            default_goal_max_cost=self._default_goal_max_cost,
            repeat_limit=self._repeat_limit,
            default_branches=self._default_branches,
            batch_executor=execute_batch,
            repository_paths=self._repository_paths,
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
            if isinstance(turn.decision, dict):
                from vuzol.leadership.notices import reject_saved_commands

                await reject_saved_commands(
                    uow.session, goal, wakeup_id, turn.decision, "decision_stale"
                )
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
    if package.managed_goal_id is not None and package.status is WorkPackageStatus.RUNNING:
        from vuzol.leadership.tasks import task_turn_ready

        return await task_turn_ready(session, package)
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


def _current_answer(constraints: dict[str, Any]) -> dict[str, Any] | None:
    answer = constraints.get("owner_answer")
    if not isinstance(answer, dict) or answer.get("superseded"):
        return None
    return answer
