"""Single command boundary for human and agent project-lead callers."""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.domain import DomainError, PlanDraft
from vuzol.leadership.actor import Actor, ActorKind, ActorRejected
from vuzol.leadership.commands import (
    DELEGATED_ACTIONS,
    DRAFT_ACTIONS,
    MEMORY_COMMANDS,
    READ_COMMANDS,
    CommandResult,
    CommandStatus,
    LeadCommand,
    LeadCommandName,
    parse_command_name,
    payload_hash,
)
from vuzol.leadership.evidence import assess_completion
from vuzol.leadership.fence import lock_project_admission
from vuzol.leadership.fencing import TurnClaim, claim_current
from vuzol.leadership.persistence import (
    CONTRACT_VERSION,
    LIVE_GOAL_LIFECYCLES,
    POLICY_REVISION,
    GoalDelegation,
    LeadershipCommandReceipt,
    ManagedGoal,
    current_request,
    has_owner_mandate,
)
from vuzol.leadership.policy import (
    PolicyDecision,
    authorize,
    delegation_is_expired,
    integration_branch_rejection,
)
from vuzol.leadership.wakeups import enabled_wakeup_key, enqueue_goal_wakeup, resume_wakeup_key
from vuzol.storage.models import Event, WorkPackage
from vuzol.storage.types import WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.application import Principal

_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_FORBIDDEN_PAYLOAD_KEYS = frozenset(
    {
        "actor",
        "agent_principal_id",
        "allowed_actions",
        "owner_user_id",
        "policy_revision",
        "user_id",
    }
)
_TERMINAL = frozenset({"completed", "cancelled"})

BatchExecutor = Callable[..., Awaitable[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class _Outcome:
    status: CommandStatus
    reason: str | None
    result: dict[str, Any]
    goal: ManagedGoal | None


class LeadershipCommandService:
    """Apply one command atomically with its receipt. No second entry point."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        enabled: bool,
        branch_prefix: str = "vuzol-managed/",
        repeat_limit: int = 2,
        default_branches: Mapping[str, str] | None = None,
        batch_executor: BatchExecutor | None = None,
        repository_paths: Mapping[str, Path] | None = None,
    ) -> None:
        self._factory = session_factory
        self._enabled = enabled
        self._branch_prefix = branch_prefix
        self._repeat_limit = repeat_limit
        self._default_branches = dict(default_branches or {})
        self._batch_executor = batch_executor
        self._repository_paths = dict(repository_paths or {})

    async def apply(
        self,
        actor: Actor,
        command: LeadCommand,
        *,
        claim: TurnClaim | None = None,
        uow: UnitOfWork | None = None,
    ) -> CommandResult:
        async def store(
            session: AsyncSession,
            actor: Actor,
            command: LeadCommand,
            digest: str,
            outcome: _Outcome,
        ) -> CommandResult:
            return await _store(
                session,
                actor,
                command,
                digest,
                outcome,
                source_wakeup_id=claim.wakeup_id if claim else None,
            )

        name = parse_command_name(command.name)
        digest = payload_hash(command)
        async with AsyncExitStack() as stack:
            if uow is None:
                uow = await stack.enter_async_context(UnitOfWork(self._factory))
            assert uow.session is not None
            # An enclosing intake transaction uses this same order before it
            # locks its discussion row. Keep project admission ahead of receipts.
            if name in {
                LeadCommandName.ENABLE_MANAGED_GOAL,
                LeadCommandName.REQUEST_PLAN,
                LeadCommandName.REVISE_PLAN,
                LeadCommandName.START_BATCH,
                LeadCommandName.START_TASK,
            }:
                await lock_project_admission(uow.session, command.project_id)
            receipt_key = (
                f"lead-command:{actor.kind.value}:{actor.subject}:{command.idempotency_key}"
            )
            await uow.session.execute(
                select(func.pg_advisory_xact_lock(func.hashtextextended(receipt_key, 0)))
            )
            existing = await _receipt(uow.session, actor, command.idempotency_key)
            if existing is not None:
                return _duplicate_or_conflict(existing, actor, command, digest)
            if name is None or command.contract_version != CONTRACT_VERSION:
                return await store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.UNSUPPORTED, "unsupported_command", {}, None),
                )
            if _FORBIDDEN_PAYLOAD_KEYS.intersection(command.payload):
                return await store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.DENIED, "caller_supplied_identity", {}, None),
                )
            goal = await _lock_goal(uow.session, command)
            # The first receipt read can race a transaction waiting on this goal lock.
            existing = await _receipt(uow.session, actor, command.idempotency_key)
            if existing is not None:
                return _duplicate_or_conflict(existing, actor, command, digest)
            if claim is not None and (
                goal is None or not await claim_current(uow.session, goal, claim)
            ):
                from vuzol.storage.errors import LeaseLost

                raise LeaseLost("project lead command claim expired")
            delegation = (
                None
                if goal is None
                else await uow.session.get(GoalDelegation, goal.delegation_id, with_for_update=True)
            )
            if (
                goal is not None
                and not has_owner_mandate(goal)
                and name.value not in DRAFT_ACTIONS
                and name not in MEMORY_COMMANDS
                and name
                not in {
                    LeadCommandName.ANSWER_PERMISSION,
                    LeadCommandName.RESUME_GOAL,
                    LeadCommandName.SET_WORK_LIMITS,
                    LeadCommandName.REVOKE_DELEGATION,
                }
            ):
                return await store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.DENIED, "owner_mandate_required", {}, goal),
                )
            decision = _decision(actor, name, command, goal, delegation, self._enabled)
            if not decision.allowed:
                return await store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus(decision.status), decision.reason, {}, goal),
                )
            version_problem = _version_problem(name, command, goal)
            if version_problem is not None:
                return await store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.STALE, version_problem, {}, goal),
                )
            try:
                outcome = await self._perform(
                    uow, actor, name, command, goal, delegation, claim=claim
                )
            except DomainError as error:
                if goal is not None:
                    await uow.session.refresh(goal)
                if goal is not None and error.code in {
                    "work_package_limit_reached",
                    "work_item_limit_reached",
                }:
                    question = (
                        "Достигнут заданный владельцем лимит работы. "
                        "Проверьте результат и явно измените лимит, если нужна доработка."
                    )
                    goal.wakeup_condition = {
                        "kind": "owner",
                        "question": question,
                        "scope_revision": goal.scope_revision,
                        "asked_version": goal.version + 1,
                    }
                    outcome = _transition(goal, actor, "waiting", error.code, CommandStatus.WAITING)
                    await _enqueue_owner_question(uow.session, goal, question, goal.version)
                else:
                    outcome = _Outcome(
                        CommandStatus.DENIED,
                        error.code,
                        {"message": str(error)} if name in MEMORY_COMMANDS else {},
                        goal,
                    )
            return await store(uow.session, actor, command, digest, outcome)

    async def apply_human(
        self, principal: Principal, command: LeadCommand, *, uow: UnitOfWork | None = None
    ) -> CommandResult:
        return await self.apply(Actor.from_principal(principal), command, uow=uow)

    async def apply_agent(
        self,
        agent_principal_id: str,
        ingress_source: str,
        command: LeadCommand,
        *,
        claim: TurnClaim | None = None,
    ) -> CommandResult:
        try:
            actor = Actor.agent(agent_principal_id, ingress_source)
        except ActorRejected as error:
            return CommandResult(
                status=CommandStatus.DENIED.value,
                actor_kind=ActorKind.AGENT.value,
                actor_subject=agent_principal_id,
                command_name=command.name,
                project_id=command.project_id,
                receipt_id=None,
                goal_id=command.goal_id,
                version=None,
                reason=str(error),
                result={},
            )
        return await self.apply(actor, command, claim=claim)

    async def _perform(
        self,
        uow: UnitOfWork,
        actor: Actor,
        name: LeadCommandName,
        command: LeadCommand,
        goal: ManagedGoal | None,
        delegation: GoalDelegation | None,
        *,
        claim: TurnClaim | None = None,
    ) -> _Outcome:
        assert uow.session is not None
        if name is LeadCommandName.ENABLE_MANAGED_GOAL:
            return await self._enable(uow, actor, command)
        if goal is None or delegation is None:
            return _Outcome(CommandStatus.DENIED, "goal_not_found", {}, None)
        if goal.lifecycle in _TERMINAL and name not in READ_COMMANDS:
            return _Outcome(CommandStatus.DENIED, "goal_terminal", {}, goal)
        if goal.lifecycle == "paused" and name not in READ_COMMANDS | MEMORY_COMMANDS | {
            LeadCommandName.REQUEST_PLAN,
            LeadCommandName.REVISE_PLAN,
            LeadCommandName.AMEND_GOAL,
            LeadCommandName.CANCEL_TASK,
            LeadCommandName.SET_WORK_LIMITS,
            LeadCommandName.RESUME_GOAL,
            LeadCommandName.CANCEL_GOAL,
            LeadCommandName.REVOKE_DELEGATION,
            LeadCommandName.ANSWER_PERMISSION,
        }:
            return _Outcome(CommandStatus.DENIED, "goal_paused", {}, goal)
        if name in READ_COMMANDS:
            body = _view(name, goal, delegation)
            if name in {
                LeadCommandName.GET_GOAL,
                LeadCommandName.GET_RESULT,
                LeadCommandName.GET_PROJECT_STATE,
            }:
                from vuzol.leadership.tasks import task_context

                body.update(await _goal_projection(uow.session, goal))
                body["task_control"] = await task_context(uow.session, goal)
            return _Outcome(CommandStatus.APPLIED, None, body, goal)
        if name in MEMORY_COMMANDS:
            from vuzol.leadership.notebooks import apply_memory_command

            note = await apply_memory_command(uow.session, goal, command)
            _bump(goal, actor)
            return _Outcome(
                CommandStatus.APPLIED,
                None,
                {"note_id": str(note.id), "version": goal.version},
                goal,
            )
        if name is LeadCommandName.PROPOSE_COMPLETION and (
            goal.constraints.get("pending_amendment") or goal.constraints.get("queued_amendment")
        ):
            return _Outcome(CommandStatus.DENIED, "amendment_pending", {}, goal)
        if name in {
            LeadCommandName.START_BATCH,
            LeadCommandName.START_TASK,
            LeadCommandName.REQUEST_PLAN,
            LeadCommandName.REVISE_PLAN,
            LeadCommandName.WAIT_FOR,
        }:
            if goal.lifecycle == "waiting" and name is LeadCommandName.WAIT_FOR:
                # Already waiting: the lead agrees to wait. Denying it turned the
                # wait into an owner question and lost the reason (pilot 2026-10-24).
                return _Outcome(CommandStatus.WAITING, None, {"already_waiting": True}, goal)
            if goal.lifecycle == "waiting":
                planning = name in {LeadCommandName.REQUEST_PLAN, LeadCommandName.REVISE_PLAN}
                from vuzol.storage.models import TransactionalOutbox

                source = (
                    await uow.session.get(TransactionalOutbox, claim.wakeup_id) if claim else None
                )
                permitted = planning and (
                    goal.waiting_reason == "plan_approval"
                    or actor.kind is ActorKind.HUMAN
                    or (
                        source is not None
                        and source.payload.get("wakeup_kind") in {"owner_message", "plan_requested"}
                    )
                )
                if not permitted:
                    return _Outcome(CommandStatus.DENIED, "goal_waiting", {}, goal)
            if goal.constraints.get("pending_amendment"):
                return _Outcome(CommandStatus.DENIED, "amendment_pending", {}, goal)
        if name is LeadCommandName.SET_WORK_LIMITS:
            from vuzol.leadership.limits import parse_work_limits

            limits = parse_work_limits(command.payload.get("work_limits"))
            goal.constraints = {**goal.constraints, "work_limits": limits.model_dump()}
            # Changing a grant never resumes work or answers an outstanding question.
            _bump(goal, actor)
            return _Outcome(CommandStatus.APPLIED, None, {"work_limits": limits.model_dump()}, goal)
        if name is LeadCommandName.PAUSE_GOAL:
            await _settle_children(uow.session, goal, actor, stop=False)
            return _transition(goal, actor, "paused", "owner_pause", CommandStatus.APPLIED)
        if name is LeadCommandName.RESUME_GOAL:
            if goal.lifecycle != "paused":
                return _Outcome(CommandStatus.DENIED, "goal_not_paused", {}, goal)
            if (
                delegation.revoked_at is not None
                or delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))
                or delegation.policy_revision != POLICY_REVISION
            ):
                return _Outcome(CommandStatus.DENIED, "delegation_inactive", {}, goal)
            try:
                async with uow.session.begin_nested():
                    await _resume_children(uow.session, goal, actor)
            except (ValueError, DomainError) as error:
                return _Outcome(CommandStatus.DENIED, str(error)[:120], {}, goal)
            goal.wakeup_condition = None
            outcome = _transition(
                goal,
                actor,
                "active" if has_owner_mandate(goal) else "draft",
                None,
                CommandStatus.APPLIED,
            )
            await enqueue_goal_wakeup(
                uow.session,
                goal_id=goal.id,
                idempotency_key=resume_wakeup_key(goal.id, goal.fence_epoch),
                package_id=goal.current_package_id,
                kind="resume",
            )
            return outcome
        if name is LeadCommandName.CANCEL_GOAL:
            await _settle_children(uow.session, goal, actor, stop=True)
            return _transition(goal, actor, "cancelled", "cancelled", CommandStatus.APPLIED)
        if name is LeadCommandName.REVOKE_DELEGATION:
            delegation.revoked_at = datetime.now(UTC)
            delegation.version += 1
            await _settle_children(uow.session, goal, actor, stop=True)
            return _transition(goal, actor, "paused", "delegation_revoked", CommandStatus.APPLIED)
        if name is LeadCommandName.REQUEST_PERMISSION:
            question = str(command.payload.get("question") or "").strip()
            if not question:
                return _Outcome(CommandStatus.DENIED, "question_required", {}, goal)
            from vuzol.leadership.telegram_commands import question_options
            from vuzol.leadership.text import is_plan_approval_question

            if goal.waiting_reason == "plan_approval" and is_plan_approval_question(question):
                # The plan card is the only approval decision. Keep its wait/fence intact.
                return _Outcome(CommandStatus.WAITING, "plan_approval", {}, goal)
            asked_version = goal.version + 1
            goal.wakeup_condition = {
                "kind": "owner",
                "question": question,
                "scope_revision": goal.scope_revision,
                "asked_version": asked_version,
                "options": question_options(question, command.payload.get("options")),
            }
            outcome = _transition(goal, actor, "waiting", "owner", CommandStatus.WAITING)
            await _enqueue_owner_question(uow.session, goal, question, asked_version)
            return outcome
        if name is LeadCommandName.ANSWER_PERMISSION:
            return await _answer_permission(uow.session, actor, command, goal)
        if name is LeadCommandName.WAIT_FOR:
            condition = command.payload.get("condition")
            if not isinstance(condition, dict) or not condition.get("kind"):
                return _Outcome(CommandStatus.DENIED, "wait_condition_required", {}, goal)
            if not _wait_supported(condition):
                return _Outcome(CommandStatus.UNSUPPORTED, "wait_condition_unsupported", {}, goal)
            package = await uow.session.get(WorkPackage, uuid.UUID(str(condition["package_id"])))
            if (
                package is None
                or package.managed_goal_id != goal.id
                or package.id != goal.current_package_id
            ):
                return _Outcome(CommandStatus.DENIED, "wait_package_out_of_scope", {}, goal)
            if await _wait_already_satisfied(uow.session, condition):
                ready_identity = f"{package.id}:{package.version}"
                if goal.constraints.get("last_ready_wait") == ready_identity:
                    outcome = _transition(
                        goal, actor, "waiting", "wait_without_new_evidence", CommandStatus.WAITING
                    )
                    await request_owner_recovery(uow.session, goal, "wait_without_new_evidence")
                    return outcome
                goal.constraints = {**goal.constraints, "last_ready_wait": ready_identity}
                goal.wakeup_condition = None
                _transition(
                    goal,
                    actor,
                    "active" if has_owner_mandate(goal) else "draft",
                    None,
                    CommandStatus.APPLIED,
                )
                await enqueue_goal_wakeup(
                    uow.session,
                    goal_id=goal.id,
                    idempotency_key=resume_wakeup_key(goal.id, goal.fence_epoch),
                    package_id=package.id,
                    kind="wait_ready",
                )
                return _Outcome(
                    CommandStatus.APPLIED,
                    "already_satisfied",
                    {"version": goal.version},
                    goal,
                )
            goal.lease_owner = None
            goal.lease_until = None
            from vuzol.leadership.batches import revision_needs_owner
            from vuzol.storage.models import PlanRevision

            revision = (
                await uow.session.get(PlanRevision, package.head_revision_id)
                if package.status is WorkPackageStatus.DRAFT and package.head_revision_id
                else None
            )
            if revision is not None and await revision_needs_owner(uow.session, revision):
                # Waiting on a draft only the owner can approve is a plan approval,
                # whatever the lead called it; the card and attention depend on it.
                goal.wakeup_condition = {"kind": "plan_approval", "revision_id": str(revision.id)}
                return _transition(goal, actor, "waiting", "plan_approval", CommandStatus.WAITING)
            goal.wakeup_condition = dict(condition)
            return _transition(goal, actor, "waiting", "wait_for", CommandStatus.WAITING)
        if name in {LeadCommandName.REQUEST_PLAN, LeadCommandName.REVISE_PLAN}:
            from vuzol.leadership.planning import apply_outcome, queue_request
            from vuzol.storage.models import TransactionalOutbox

            source = await uow.session.get(TransactionalOutbox, claim.wakeup_id) if claim else None
            if source is not None and source.payload.get("wakeup_kind") == "plan_requested":
                assert claim is not None
                result = await apply_outcome(
                    uow,
                    goal,
                    claim,
                    repository=self._repository_paths.get(goal.project_id),
                )
            else:
                result = await queue_request(uow.session, goal, command)
                if source is not None and source.payload.get("wakeup_kind") == "owner_message":
                    requested = await uow.session.get(
                        TransactionalOutbox, uuid.UUID(result["planner_request_id"])
                    )
                    assert requested is not None
                    requested.payload = {**requested.payload, "owner_corrections": source.payload}
            _bump(goal, actor)
            return _Outcome(CommandStatus.APPLIED, None, {"version": goal.version, **result}, goal)
        if name in {LeadCommandName.START_BATCH, LeadCommandName.START_TASK}:
            from vuzol.leadership.planning import pending_request

            if await pending_request(uow.session, goal):
                raise DomainError("planner_request_pending")
            if self._batch_executor is None:
                return _Outcome(CommandStatus.UNSUPPORTED, "batch_executor_unbound", {}, goal)
            async with uow.session.begin_nested():
                produced = await self._batch_executor(
                    uow,
                    actor=actor,
                    goal=goal,
                    delegation=delegation,
                    command=command,
                    repeat_limit=self._repeat_limit,
                )
            status = CommandStatus(str(produced.pop("status", CommandStatus.APPLIED.value)))
            reason = produced.pop("reason", None)
            return _Outcome(status, None if reason is None else str(reason), produced, goal)
        if name is LeadCommandName.CANCEL_TASK:
            from vuzol.leadership.tasks import scoped_task

            task = await scoped_task(uow.session, goal, command.payload.get("task_id"))
            await _control_task(uow.session, task.id, actor, stop=True)
            _bump(goal, actor)
            return _Outcome(CommandStatus.APPLIED, None, {"task_id": str(task.id)}, goal)
        if name is LeadCommandName.ACCEPT_RESULT:
            from vuzol.leadership.tasks import accept_result

            async with uow.session.begin_nested():
                accepted = await accept_result(
                    uow.session,
                    goal,
                    command.payload,
                    actor_id=actor.subject,
                    branch_prefix=self._branch_prefix,
                )
            _bump(goal, actor)
            return _Outcome(CommandStatus.APPLIED, None, accepted, goal)
        if name is LeadCommandName.AMEND_GOAL:
            from vuzol.leadership.amendments import store_amendment

            amendment = await store_amendment(
                uow.session, goal, delegation, command.payload, reason=command.reason
            )
            if amendment["inflight_policy"] == "cancel_current":
                await _settle_children(uow.session, goal, actor, stop=True)
            _bump(goal, actor)
            await enqueue_goal_wakeup(
                uow.session,
                goal_id=goal.id,
                package_id=goal.current_package_id,
                idempotency_key=f"lead:amendment:{goal.id}:{goal.version}",
                kind="amendment",
            )
            return _Outcome(CommandStatus.APPLIED, None, {"amendment": amendment}, goal)
        if name is LeadCommandName.PROPOSE_COMPLETION:
            from vuzol.leadership.evidence import _current_head

            rework_from = goal.constraints.get("rework_from_head")
            if rework_from is not None and await _current_head(uow.session, goal) == rework_from:
                # The owner asked for more on this exact result (final goal card).
                return _Outcome(CommandStatus.DENIED, "rework_pending", {}, goal)
            assessment = await assess_completion(uow.session, goal, check_live_head=True)
            if assessment.satisfied:
                goal.constraints = {
                    key: value
                    for key, value in goal.constraints.items()
                    if key != "rework_from_head"
                }
                previous = goal.lifecycle
                goal.lifecycle = "completed"
                goal.waiting_reason = None
                goal.lease_owner = None
                goal.lease_until = None
                _bump(goal, actor)
                _event(
                    uow.session,
                    goal,
                    actor,
                    "managed_goal.completion_proposed",
                    previous,
                    goal.lifecycle,
                    extra={"covered": list(assessment.covered)},
                )
                return _Outcome(
                    CommandStatus.APPLIED,
                    None,
                    {
                        "version": goal.version,
                        "lifecycle": goal.lifecycle,
                        "covered": list(assessment.covered),
                    },
                    goal,
                )
            question = (
                "Не подтверждены критерии: "  # noqa: RUF001
                + ", ".join(assessment.missing)
                + ". Уточните следующий шаг."
            )
            goal.wakeup_condition = {
                "kind": "owner",
                "missing": list(assessment.missing),
                "question": question,
                "scope_revision": goal.scope_revision,
                "asked_version": goal.version + 1,
            }
            outcome = _transition(
                goal, actor, "waiting", "evidence_not_recorded", CommandStatus.WAITING
            )
            await _enqueue_owner_question(uow.session, goal, question, goal.version)
            return outcome
        if name in {
            LeadCommandName.ACCEPT_CRITERION,
            LeadCommandName.ACCEPT_UNCERTAINTY,
            LeadCommandName.ADMIT_CEILING,
            LeadCommandName.RECONCILE_COST,
        }:
            return _Outcome(CommandStatus.DENIED, "owner_decision_required", {}, goal)
        return _Outcome(CommandStatus.UNSUPPORTED, "unsupported_command", {}, goal)

    async def _enable(self, uow: UnitOfWork, actor: Actor, command: LeadCommand) -> _Outcome:
        assert uow.session is not None
        if actor.user_id is None:
            return _Outcome(CommandStatus.DENIED, "owner_decision_required", {}, None)
        session_raw = command.payload.get("session_id")
        try:
            session_id = uuid.UUID(str(session_raw))
        except (TypeError, ValueError):
            return _Outcome(CommandStatus.DENIED, "session_required", {}, None)
        try:
            discussion = await uow.discussions.get_session(session_id, for_update=True)
        except LookupError:
            return _Outcome(CommandStatus.DENIED, "session_required", {}, None)
        if discussion.project_id != command.project_id:
            return _Outcome(CommandStatus.DENIED, "project_mismatch", {}, None)
        live = await uow.session.scalar(
            select(ManagedGoal.id).where(
                ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
                (ManagedGoal.project_id == command.project_id)
                | (ManagedGoal.discussion_session_id == session_id),
            )
        )
        if live is not None:
            return _Outcome(CommandStatus.DENIED, "goal_already_live", {}, None)
        live_package = await uow.session.scalar(
            select(WorkPackage.id)
            .where(
                WorkPackage.project_id == command.project_id,
                WorkPackage.status.in_(
                    (
                        WorkPackageStatus.DRAFT,
                        WorkPackageStatus.APPROVED,
                        WorkPackageStatus.RUNNING,
                        WorkPackageStatus.PAUSED,
                    )
                ),
            )
            .limit(1)
        )
        if live_package is not None:
            return _Outcome(CommandStatus.DENIED, "package_already_live", {}, None)
        branch = str(command.payload.get("integration_branch") or "")
        rejection = integration_branch_rejection(
            branch,
            prefix=self._branch_prefix,
            default_branch=self._default_branches.get(command.project_id),
        )
        if rejection is not None:
            return _Outcome(CommandStatus.DENIED, rejection, {}, None)
        criteria = _criteria(command.payload.get("criteria"))
        request = str(command.payload.get("request") or "").strip()
        scope = str(command.payload.get("scope") or request).strip()
        if not request or not scope:
            return _Outcome(CommandStatus.DENIED, "scope_required", {}, None)
        from vuzol.leadership.limits import parse_work_limits

        work_limits = parse_work_limits(command.payload.get("work_limits", {}))
        principal = f"agent:project-lead:{command.project_id}"
        delegation = GoalDelegation(
            project_id=command.project_id,
            owner_user_id=actor.user_id,
            agent_principal_id=principal,
            policy_revision=POLICY_REVISION,
            allowed_actions=sorted(DELEGATED_ACTIONS - {"propose_batch", "request_revision"}),
            integration_branch=branch,
            environments=["isolated"],
        )
        uow.session.add(delegation)
        await uow.session.flush()
        default_branch = self._default_branches.get(command.project_id)
        constraints: dict[str, Any] = {"work_limits": work_limits.model_dump()}
        if default_branch:
            constraints["project_default_branch"] = default_branch
        goal = ManagedGoal(
            project_id=command.project_id,
            owner_user_id=actor.user_id,
            discussion_session_id=discussion.id,
            delegation_id=delegation.id,
            source_chat_id=discussion.chat_id,
            source_thread_id=discussion.message_thread_id,
            original_request=request,
            scope_text=scope,
            criteria=criteria,
            constraints=constraints,
            lifecycle="active",
            integration_branch=branch,
        )
        uow.session.add(goal)
        await uow.session.flush()
        from vuzol.leadership.provenance import RequestOrigin, record_incoming_request

        ingress = actor.ingress_source
        origin = RequestOrigin.model_validate(
            command.payload.get("request_origin")
            or {
                "channel": ingress if ingress in {"telegram", "cli"} else "api",
                "display_name": "Telegram" if ingress == "telegram" else ingress,
            }
        )
        await record_incoming_request(uow.session, goal, origin)
        await enqueue_goal_wakeup(
            uow.session,
            goal_id=goal.id,
            idempotency_key=enabled_wakeup_key(goal.id),
            package_id=None,
            kind="enabled",
        )
        _event(uow.session, goal, actor, "managed_goal.enabled", None, "active")
        return _Outcome(
            CommandStatus.APPLIED,
            None,
            {
                "goal_id": str(goal.id),
                "version": goal.version,
                "agent_principal_id": principal,
                "integration_branch": branch,
            },
            goal,
        )


def _decision(
    actor: Actor,
    name: LeadCommandName,
    command: LeadCommand,
    goal: ManagedGoal | None,
    delegation: GoalDelegation | None,
    feature_enabled: bool,
) -> PolicyDecision:
    now = datetime.now(UTC)
    actions: frozenset[str] = frozenset()
    revoked = False
    expired = False
    policy_revision = None
    delegation_project = None
    delegation_principal = None
    if delegation is not None:
        actions = frozenset(str(item) for item in delegation.allowed_actions)
        revoked = delegation.revoked_at is not None
        expired = delegation_is_expired(delegation.expires_at, now=now)
        policy_revision = delegation.policy_revision
        delegation_project = delegation.project_id
        delegation_principal = delegation.agent_principal_id
    if command.name in {"propose_batch", "propose_plan"} and not actions.intersection(
        {"propose_batch", "propose_plan"}
    ):
        return PolicyDecision(False, "denied", "action_not_delegated")
    return authorize(
        actor,
        name,
        feature_enabled=feature_enabled,
        project_id=command.project_id,
        goal_project_id=None if goal is None else goal.project_id,
        goal_owner_user_id=None if goal is None else goal.owner_user_id,
        delegation_project_id=delegation_project,
        delegation_agent_principal_id=delegation_principal,
        delegation_actions=actions,
        delegation_revoked=revoked,
        delegation_expired=expired,
        delegation_policy_revision=policy_revision,
    )


def _version_problem(
    name: LeadCommandName, command: LeadCommand, goal: ManagedGoal | None
) -> str | None:
    if name is LeadCommandName.ENABLE_MANAGED_GOAL or name in READ_COMMANDS:
        if (
            name in READ_COMMANDS
            and command.expected_version is not None
            and goal is not None
            and command.expected_version != goal.version
        ):
            return "stale_version"
        return None
    if goal is None:
        return None
    if command.expected_version is None:
        return "version_required"
    if command.expected_version != goal.version:
        return "stale_version"
    return None


def _transition(
    goal: ManagedGoal,
    actor: Actor,
    lifecycle: str,
    reason: str | None,
    status: CommandStatus,
) -> _Outcome:
    previous = goal.lifecycle
    goal.lifecycle = lifecycle
    goal.waiting_reason = reason
    if lifecycle in {"paused", "waiting", "cancelled"}:
        goal.lease_owner = None
        goal.lease_until = None
    _bump(goal, actor)
    return _Outcome(
        status,
        reason,
        {"version": goal.version, "lifecycle": goal.lifecycle, "previous": previous},
        goal,
    )


def _bump(goal: ManagedGoal, actor: Actor) -> None:
    del actor
    goal.version += 1
    goal.fence_epoch += 1


async def goal_card_view(
    session: AsyncSession, goal: ManagedGoal, delegation: GoalDelegation
) -> dict[str, Any]:
    """Projection delivered to the owner. It includes the checker result, not a plan id."""

    body = _view(LeadCommandName.GET_GOAL, goal, delegation)
    body.update(await _goal_projection(session, goal))
    return body


async def _answer_permission(
    session: AsyncSession, actor: Actor, command: LeadCommand, goal: ManagedGoal
) -> _Outcome:
    answer = str(command.payload.get("answer") or "").strip()
    if not answer:
        return _Outcome(CommandStatus.DENIED, "answer_required", {}, goal)
    condition = goal.wakeup_condition if isinstance(goal.wakeup_condition, dict) else {}
    question = condition.get("question")
    if condition.get("kind") != "owner" or not isinstance(question, str) or not question.strip():
        return _Outcome(CommandStatus.DENIED, "question_closed", {}, goal)
    if condition.get("scope_revision") != goal.scope_revision:
        return _Outcome(CommandStatus.DENIED, "question_revision_mismatch", {}, goal)
    if command.payload.get("question_version") != condition.get("asked_version"):
        return _Outcome(CommandStatus.STALE, "question_version_mismatch", {}, goal)
    constraints = dict(goal.constraints)
    constraints["owner_answer"] = {
        "answer": answer,
        "question": question.strip(),
        "scope_revision": goal.scope_revision,
        "question_version": condition.get("asked_version"),
        "version": goal.version,
    }
    goal.constraints = constraints
    goal.wakeup_condition = None
    if goal.lifecycle == "paused":
        _bump(goal, actor)
        return _Outcome(
            CommandStatus.APPLIED,
            None,
            {"version": goal.version, "lifecycle": goal.lifecycle},
            goal,
        )
    outcome = _transition(
        goal, actor, "active" if has_owner_mandate(goal) else "draft", None, CommandStatus.APPLIED
    )
    await enqueue_goal_wakeup(
        session,
        goal_id=goal.id,
        idempotency_key=resume_wakeup_key(goal.id, goal.fence_epoch),
        package_id=goal.current_package_id,
        kind="resume",
    )
    return outcome


async def enqueue_goal_status(session: AsyncSession, goal: ManagedGoal) -> None:
    from vuzol.leadership.card import render_goal_card
    from vuzol.leadership.telegram_commands import goal_status_buttons
    from vuzol.telegram.attention import sync_goal_attention
    from vuzol.telegram.projections import enqueue_chat_notice

    delegation = await session.get(GoalDelegation, goal.delegation_id)
    if delegation is None:
        return
    view = await goal_card_view(session, goal, delegation)
    text = render_goal_card(view)
    await sync_goal_attention(session, goal, completion_confirmed=view["completion_confirmed"])
    await enqueue_chat_notice(
        session,
        chat_id=goal.source_chat_id,
        thread_id=goal.source_thread_id,
        key=f"lead:state:{goal.id}:{goal.version}",
        text=text,
        markdown=True,
        projection_role=f"ls:{goal.id.hex}",
        revision=goal.version,
        callback_buttons=goal_status_buttons(goal),
    )
    if goal.lifecycle in {"completed", "cancelled"}:
        from vuzol.leadership.acceptance import acceptance_ready
        from vuzol.leadership.telegram_commands import final_card_buttons

        # The finished card is the owner's one decision on the goal: it arrives at the
        # bottom of the topic and is edited in place once accepted.
        await enqueue_chat_notice(
            session,
            chat_id=goal.source_chat_id,
            thread_id=goal.source_thread_id,
            key=f"lead:finished:{goal.id}:{goal.version}",
            text=text,
            markdown=True,
            projection_role=f"lf:{goal.id.hex}",
            revision=goal.version,
            callback_buttons=final_card_buttons(goal, ready=await acceptance_ready(session, goal)),
        )


async def _enqueue_owner_question(
    session: AsyncSession, goal: ManagedGoal, question: str, version: int
) -> None:
    from vuzol.leadership.card import render_question
    from vuzol.leadership.telegram_commands import question_buttons
    from vuzol.telegram.projections import enqueue_chat_notice

    await enqueue_chat_notice(
        session,
        chat_id=goal.source_chat_id,
        thread_id=goal.source_thread_id,
        key=f"lead:question:{goal.id}:{version}",
        text=render_question(question, version, has_options=bool(question_buttons(goal))),
        markdown=True,
        projection_role=f"lq:{goal.id.hex}:{version}",
        revision=version,
        callback_buttons=question_buttons(goal),
    )
    await enqueue_goal_status(session, goal)


_STEP_NAMES = {
    "interpret": "разбора задачи",
    "prepare_context": "подготовки",
    "prepare_worktree": "подготовки",
    "plan": "планирования",
    "execute_code": "работы исполнителя",
    "execute_agent": "работы исполнителя",
    "validate": "проверок",
    "review": "ревью",
    "approval": "приёмки",
    "apply_result": "применения результата",
    "publish_static": "публикации",
    "finalize": "завершения",
}
_FAILURES = {
    "shutdown_cancelled": "сервис обновлялся во время {step}",
    "lease_expired_attempts_exhausted": "несколько попыток {step} не успели завершиться",
    "provider_unavailable": "модель была недоступна во время {step}",
    "provider_request_invalid": "модель отклонила запрос во время {step}",
    "budget_exhausted": "закончился бюджет пункта во время {step}",
    "no_compatible_profile": "нет подходящей модели для {step}",
    "approved_result_not_applied": "одобренный результат не удалось применить",
    "reviewer_output_invalid": "модель-ревьюер несколько раз ответила в неверном формате",
    "independent_review_required": "независимое ревью не дало заключения",
}


async def _stopped_item_question(session: AsyncSession, goal: ManagedGoal) -> str | None:
    """Say which item stopped, why, and which button continues it."""
    from vuzol.leadership.text import short_owner_text
    from vuzol.storage.models import PlanRevision, Run, Step
    from vuzol.storage.types import StepStatus, WorkPackagePauseReason
    from vuzol.telegram.work_package_projections import package_retry_available

    package = (
        await session.get(WorkPackage, goal.current_package_id) if goal.current_package_id else None
    )
    if (
        package is None
        or package.status is not WorkPackageStatus.PAUSED
        or package.pause_reason
        not in {WorkPackagePauseReason.ITEM_BLOCKED, WorkPackagePauseReason.ITEM_FAILED}
    ):
        return None
    revision = (
        await session.get(PlanRevision, package.head_revision_id)
        if package.head_revision_id
        else None
    )
    items = [] if revision is None else revision.immutable_body.get("items", [])
    ordinal = package.cursor_ordinal
    item = next((i for i in items if i.get("ordinal") == ordinal), None)
    step = (
        None
        if package.last_failure_task_id is None
        else await session.scalar(
            select(Step)
            .join(Run, Run.id == Step.run_id)
            .where(
                Run.task_id == package.last_failure_task_id,
                Step.status.in_((StepStatus.BLOCKED, StepStatus.FAILED)),
            )
            .order_by(Step.updated_at.desc())
            .limit(1)
        )
    )
    name = "пункт"
    if ordinal is not None:
        name = f"Пункт {ordinal} из {len(items)}" if items else f"Пункт {ordinal}"
    if item is not None and item.get("goal"):
        name += f" «{short_owner_text(item['goal'], 90)}»"
    if step is None:
        why = "не завершился"
    else:
        where = _STEP_NAMES.get(step.step_type, "работы")
        why = _FAILURES.get(step.failure_category or "", "не завершился на этапе {step}").format(
            step=where
        )
    if await package_retry_available(session, package):
        action = (
            "Карточка плана сразу над этим сообщением: «Повторить» запустит пункт заново, "
            "«Перепланировать» попросит новый план. Или напиши, что сделать иначе."
        )
    else:
        action = (
            "Этот пункт повторить нельзя: в карточке плана над этим сообщением можно "
            "перепланировать или завершить работу. Или напиши, что сделать иначе."
        )
    return f"{name} остановился: {why}. {action}"


async def request_owner_recovery(session: AsyncSession, goal: ManagedGoal, reason: str) -> None:
    from vuzol.leadership.card import reason_text
    from vuzol.storage.models import TransactionalOutbox
    from vuzol.storage.types import DeliveryStatus

    stopped = await _stopped_item_question(session, goal)
    if stopped is not None:
        assert goal.current_package_id is not None
        # Live pilot: the card with "Повторить" was far up the topic, and the
        # question only said "уточни следующий шаг". Show both together.
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="repost_plan",
                linked_entity_type="work_package",
                linked_entity_id=goal.current_package_id,
                idempotency_key=f"wp:repost:recovery:{goal.id}:{goal.version}",
                payload={"package_id": str(goal.current_package_id)},
                status=DeliveryStatus.PENDING,
            )
        )
    if stopped is not None:
        question = stopped
    elif reason == "continuation_without_progress":
        question = (
            "Несколько ходов подряд нет нового результата и запуска работы. Уточните следующий шаг."
        )
    else:
        question = f"{reason_text(reason)} Уточни следующий шаг после устранения причины."
    goal.wakeup_condition = {
        "kind": "owner",
        "question": question,
        "scope_revision": goal.scope_revision,
        "asked_version": goal.version,
    }
    await _enqueue_owner_question(session, goal, question, goal.version)


async def _goal_projection(session: AsyncSession, goal: ManagedGoal) -> dict[str, Any]:
    from vuzol.leadership.card import open_question, owner_answer_text
    from vuzol.leadership.execution_view import execution_snapshot

    package = None
    if goal.current_package_id is not None:
        package = await session.get(WorkPackage, goal.current_package_id)
    assessment = await assess_completion(session, goal)
    question = open_question(goal.wakeup_condition)
    if assessment.covered:
        result = "покрыто: " + ", ".join(
            str(item["description"]) for item in goal.criteria if item["id"] in assessment.covered
        )
        if package is not None and package.integration_head_commit:
            result += f"; commit {package.integration_head_commit[:12]}"
    elif package is not None and package.integration_head_commit:
        result = f"{package.integration_head_commit[:12]}, доказательств нет"
    else:
        result = "доказательств нет"
    package_line = None if package is None else f"{package.status.value} {str(package.id)[:8]}"
    from vuzol.storage.models import MaterializationLink, PlanRevisionItem, Task

    progress = []
    if package is not None:
        from vuzol.storage.models import PlanRevision

        revision = (
            await session.get(PlanRevision, package.head_revision_id)
            if package.head_revision_id
            else None
        )
        goals = {
            item.get("ordinal"): item.get("goal")
            for item in ([] if revision is None else revision.immutable_body.get("items", []))
        }
        rows = await session.execute(
            select(PlanRevisionItem.summary, Task.status, PlanRevisionItem.ordinal)
            .outerjoin(
                MaterializationLink,
                (
                    (MaterializationLink.plan_revision_id == PlanRevisionItem.plan_revision_id)
                    & (MaterializationLink.ordinal == PlanRevisionItem.ordinal)
                ),
            )
            .outerjoin(Task, Task.id == MaterializationLink.task_id)
            .where(PlanRevisionItem.plan_revision_id == package.head_revision_id)
            .order_by(PlanRevisionItem.ordinal)
        )
        progress = [
            {
                "summary": summary,
                "goal": goals.get(ordinal) or summary,
                "status": status.value if status else "planned",
            }
            for summary, status, ordinal in rows
        ]
    return {
        "request": current_request(goal),
        "scope": goal.scope_text,
        "criteria": list(goal.criteria),
        "goal_id": str(goal.id),
        "package_line": package_line,
        "package_title": None if package is None else package.title,
        "current_package_status": None if package is None else package.status.value,
        "execution": await execution_snapshot(session, goal.current_package_id),
        "progress": progress,
        "result_commit": None if package is None else package.integration_head_commit,
        "completion_confirmed": assessment.satisfied,
        "next_operation": _next_operation(goal, package, question),
        "result_summary": result,
        "question": question,
        "question_version": (goal.wakeup_condition or {}).get("asked_version")
        if question
        else None,
        "owner_answer": owner_answer_text(goal.constraints),
        "owner_acceptance": goal.constraints.get("owner_acceptance"),
        "work_limits": goal.constraints.get("work_limits", {}),
        "covered": list(assessment.covered),
        "missing": list(assessment.missing),
    }


def _next_operation(goal: ManagedGoal, package: WorkPackage | None, question: str | None) -> str:
    if goal.lifecycle == "completed":
        return "цель закрыта"
    if goal.lifecycle == "cancelled":
        return "цель отменена"
    if question:
        return "ответ владельца"
    if goal.lifecycle == "paused":
        return "пауза"
    if package is not None:
        return f"пакет {package.status.value}"
    return "решение руководителя"


def _view(name: LeadCommandName, goal: ManagedGoal, delegation: GoalDelegation) -> dict[str, Any]:
    if name is LeadCommandName.GET_CAPABILITIES:
        return {
            "actions": sorted(set(delegation.allowed_actions) | {c.value for c in MEMORY_COMMANDS}),
            "integration_branch": goal.integration_branch,
            "environments": list(delegation.environments),
            "unsupported": ["research", "deploy", "host_admin", "secrets"],
        }
    if name is LeadCommandName.GET_RESULT:
        return {
            "criteria": list(goal.criteria),
            "current_package_id": _uuid(goal.current_package_id),
            "lifecycle": goal.lifecycle,
        }
    return {
        "goal_id": str(goal.id),
        "project_id": goal.project_id,
        "lifecycle": goal.lifecycle,
        "version": goal.version,
        "scope_revision": goal.scope_revision,
        "fence_epoch": goal.fence_epoch,
        "waiting_reason": goal.waiting_reason,
        "current_package_id": _uuid(goal.current_package_id),
        "integration_branch": goal.integration_branch,
    }


def _criteria(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise DomainError("criteria_required")
    seen: set[str] = set()
    criteria: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            raise DomainError("criteria_invalid")
        criterion_id = str(item.get("id") or "")
        description = str(item.get("description") or "").strip()
        if _SLUG.fullmatch(criterion_id) is None or not description:
            raise DomainError("criteria_invalid")
        if criterion_id in seen:
            raise DomainError("criteria_invalid")
        seen.add(criterion_id)
        criteria.append({"id": criterion_id, "description": description})
    return criteria


def plan_draft_from_payload(payload: Mapping[str, Any]) -> PlanDraft:
    return _plan_draft(payload)


def _plan_draft(payload: Mapping[str, Any]) -> PlanDraft:
    raw = payload.get("plan")
    if not isinstance(raw, dict):
        raise DomainError("plan_required")
    items_raw = raw.get("items")
    if not isinstance(items_raw, list):
        raise DomainError("plan_required")
    # Reuse the revision parser; scope/commit bindings are supplied by server.
    from vuzol.discussion.service import _plan_from_revision_body

    for item in items_raw:
        if not isinstance(item, dict) or not isinstance(item.get("local_id"), str):
            raise DomainError("invalid_plan", "stable local_id is required")
        for field in ("summary", "goal", "expected_outcome", "allowed_scope"):
            if not isinstance(item.get(field), str) or not item[field].strip():
                raise DomainError("invalid_plan", f"{field} must be a non-empty string")
        for field in ("needs_approval", "derived"):
            if field in item and not isinstance(item[field], bool):
                raise DomainError("invalid_plan", f"{field} must be boolean")
        for field in ("completion_criteria", "dependencies", "out_of_scope", "trusted_checks"):
            values = item.get(field, [] if field in {"out_of_scope", "trusted_checks"} else None)
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                raise DomainError("invalid_plan", f"{field} must be a list of strings")
    return _plan_from_revision_body(raw)


async def _lock_goal(session: AsyncSession, command: LeadCommand) -> ManagedGoal | None:
    if command.name == LeadCommandName.ENABLE_MANAGED_GOAL.value:
        return None
    if command.goal_id is not None:
        return cast(
            ManagedGoal | None,
            await session.scalar(
                select(ManagedGoal).where(ManagedGoal.id == command.goal_id).with_for_update()
            ),
        )
    return cast(
        ManagedGoal | None,
        await session.scalar(
            select(ManagedGoal)
            .where(
                ManagedGoal.project_id == command.project_id,
                ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
            )
            .with_for_update()
        ),
    )


async def _receipt(
    session: AsyncSession, actor: Actor, idempotency_key: str
) -> LeadershipCommandReceipt | None:
    return cast(
        LeadershipCommandReceipt | None,
        await session.scalar(
            select(LeadershipCommandReceipt)
            .where(
                LeadershipCommandReceipt.actor_kind == actor.kind.value,
                LeadershipCommandReceipt.actor_subject == actor.subject,
                LeadershipCommandReceipt.idempotency_key == idempotency_key,
            )
            .with_for_update()
        ),
    )


def _duplicate_or_conflict(
    existing: LeadershipCommandReceipt,
    actor: Actor,
    command: LeadCommand,
    digest: str,
) -> CommandResult:
    if existing.payload_hash != digest or parse_command_name(
        existing.command_name
    ) != parse_command_name(command.name):
        return CommandResult(
            status=CommandStatus.DENIED.value,
            actor_kind=actor.kind.value,
            actor_subject=actor.subject,
            command_name=command.name,
            project_id=command.project_id,
            receipt_id=existing.id,
            goal_id=existing.goal_id,
            version=_version_of(existing.result),
            reason="idempotency_conflict",
            result={"original_status": existing.status},
        )
    return CommandResult(
        status=CommandStatus.DUPLICATE.value,
        actor_kind=actor.kind.value,
        actor_subject=actor.subject,
        command_name=existing.command_name,
        project_id=existing.project_id,
        receipt_id=existing.id,
        goal_id=existing.goal_id,
        version=_version_of(existing.result),
        reason="duplicate",
        result=dict(existing.result),
    )


async def _store(
    session: AsyncSession,
    actor: Actor,
    command: LeadCommand,
    digest: str,
    outcome: _Outcome,
    *,
    source_wakeup_id: uuid.UUID | None = None,
) -> CommandResult:
    goal_id = command.goal_id if outcome.goal is None else outcome.goal.id
    if outcome.result.get("goal_id"):
        goal_id = uuid.UUID(str(outcome.result["goal_id"]))
    receipt = LeadershipCommandReceipt(
        actor_kind=actor.kind.value,
        actor_subject=actor.subject,
        ingress_source=actor.ingress_source,
        command_name=command.name,
        project_id=command.project_id,
        goal_id=goal_id,
        expected_version=command.expected_version,
        idempotency_key=command.idempotency_key,
        payload_hash=digest,
        payload=dict(command.payload),
        contract_version=command.contract_version,
        status=outcome.status.value,
        result=dict(outcome.result),
        reason=outcome.reason,
    )
    session.add(receipt)
    await session.flush()
    from vuzol.leadership.notices import command_notice

    await command_notice(session, receipt, source_wakeup_id, outcome.goal)
    if outcome.goal is not None:
        if outcome.status is CommandStatus.APPLIED and command.name in {
            "resume_goal",
            "answer_permission",
            "amend_goal",
        }:
            from vuzol.telegram.attention import sync_attention

            await sync_attention(session, outcome.goal, None, scope="planner")
        if outcome.status is CommandStatus.WAITING and outcome.goal.wakeup_condition is None:
            await request_owner_recovery(session, outcome.goal, outcome.reason or "waiting")
        _event(
            session,
            outcome.goal,
            actor,
            f"managed_goal.command.{outcome.status.value}",
            None,
            outcome.goal.lifecycle,
            extra={"command": command.name, "reason": outcome.reason},
        )
        if outcome.status in {
            CommandStatus.APPLIED,
            CommandStatus.WAITING,
        } and command.name not in {item.value for item in READ_COMMANDS}:
            await enqueue_goal_status(session, outcome.goal)
    await session.flush()
    return CommandResult(
        status=outcome.status.value,
        actor_kind=actor.kind.value,
        actor_subject=actor.subject,
        command_name=command.name,
        project_id=command.project_id,
        receipt_id=receipt.id,
        goal_id=goal_id,
        version=outcome.goal.version if outcome.goal is not None else _version_of(outcome.result),
        reason=outcome.reason,
        result=dict(outcome.result),
    )


def _event(
    session: AsyncSession,
    goal: ManagedGoal,
    actor: Actor,
    event_type: str,
    previous: str | None,
    new_state: str,
    *,
    extra: dict[str, Any] | None = None,
) -> None:
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type=event_type,
            actor_type=actor.kind.value,
            actor_id=actor.subject,
            previous_state=previous,
            new_state=new_state,
            payload={
                "project_id": goal.project_id,
                "fence_epoch": goal.fence_epoch,
                **(extra or {}),
            },
        )
    )


def _wait_supported(condition: Mapping[str, Any]) -> bool:
    """Only a package id has a consumer. Other stored conditions would wait forever."""

    if condition.get("kind") != "package":
        return False
    try:
        uuid.UUID(str(condition.get("package_id")))
    except (TypeError, ValueError):
        return False
    return True


async def _settle_children(
    session: AsyncSession, goal: ManagedGoal, actor: Actor, *, stop: bool
) -> None:
    """Pause or stop the goal's live packages and their tasks in this transaction.

    Already finished packages stay as they are. This records the control; it does
    not promise to undo an effect that already happened.
    """

    from vuzol.discussion.domain import WorkPackageEvent
    from vuzol.storage.models import MaterializationLink, Task
    from vuzol.storage.types import USER_TERMINAL_TASK_STATUSES, WorkPackagePauseReason

    live = (
        WorkPackageStatus.DRAFT,
        WorkPackageStatus.APPROVED,
        WorkPackageStatus.RUNNING,
        WorkPackageStatus.PAUSED,
    )
    packages = (
        await session.scalars(
            select(WorkPackage)
            .where(
                WorkPackage.managed_goal_id == goal.id,
                WorkPackage.status.in_(live),
            )
            .with_for_update()
        )
    ).all()
    paused_children = dict(goal.constraints.get("paused_children", {}))
    for package in packages:
        pausable = {
            WorkPackageStatus.DRAFT,
            WorkPackageStatus.APPROVED,
            WorkPackageStatus.RUNNING,
        }
        if package.status not in pausable and not (
            stop and package.status is WorkPackageStatus.PAUSED
        ):
            continue
        previous = package.status
        task_states = {}
        links = (
            await session.scalars(
                select(MaterializationLink).where(MaterializationLink.work_package_id == package.id)
            )
        ).all()
        for link in links:
            task = await session.get(Task, link.task_id)
            if task is None or task.status in USER_TERMINAL_TASK_STATUSES:
                continue
            task_states[str(task.id)] = task.status.value
            await _control_task(session, task.id, actor, stop=stop)
        if stop:
            if package.status in {
                WorkPackageStatus.COMPLETED,
                WorkPackageStatus.STOPPED,
                WorkPackageStatus.DISCARDED,
            }:
                continue
            package.status = WorkPackageStatus.STOPPED
            package.pause_reason = None
            package.version += 1
            event_type = WorkPackageEvent.PACKAGE_STOPPED.value
            new_state = WorkPackageStatus.STOPPED.value
        else:
            paused_children[str(package.id)] = {
                "status": previous.value,
                "tasks": task_states,
            }
            package.status = WorkPackageStatus.PAUSED
            package.pause_reason = WorkPackagePauseReason.USER
            package.version += 1
            event_type = WorkPackageEvent.PACKAGE_PAUSED.value
            new_state = WorkPackageStatus.PAUSED.value
        session.add(
            Event(
                entity_type="work_package",
                entity_id=package.id,
                event_type=event_type,
                actor_type="user" if actor.user_id is not None else "system",
                actor_id=actor.subject,
                previous_state=previous.value,
                new_state=new_state,
                payload={
                    "reason": "owner_stop" if stop else WorkPackagePauseReason.USER.value,
                    "status_generation": package.version,
                    "managed_goal_id": str(goal.id),
                },
            )
        )
    goal.constraints = {**goal.constraints, "paused_children": {} if stop else paused_children}


async def _resume_children(session: AsyncSession, goal: ManagedGoal, actor: Actor) -> None:
    from vuzol.leadership.evidence import package_has_unknown_effects
    from vuzol.storage.models import Run, Task
    from vuzol.storage.types import TaskStatus, WorkPackagePauseReason
    from vuzol.workflows.controls import resume_task
    from vuzol.workflows.transitions import transition_task

    for package_id, state in goal.constraints.get("paused_children", {}).items():
        package = await session.get(WorkPackage, uuid.UUID(package_id), with_for_update=True)
        if (
            package is None
            or package.managed_goal_id != goal.id
            or package.status is not WorkPackageStatus.PAUSED
            or package.pause_reason is not WorkPackagePauseReason.USER
        ):
            continue
        if await package_has_unknown_effects(session, package.id):
            raise DomainError("unknown_effects_open")
        for task_id, previous in state["tasks"].items():
            task = await session.get(Task, uuid.UUID(task_id), with_for_update=True)
            if task is None or task.status is not TaskStatus.PAUSED:
                continue
            run = await session.scalar(select(Run).where(Run.task_id == task.id).limit(1))
            if run is None:
                await transition_task(
                    session, task, TaskStatus(previous), actor_type="user", actor_id=actor.subject
                )
            else:
                await resume_task(session, task.id, actor_id=actor.subject)
        package.status = WorkPackageStatus(state["status"])
        package.pause_reason = None
        package.version += 1
        session.add(
            Event(
                entity_type="work_package",
                entity_id=package.id,
                event_type="work_package.resumed",
                actor_type=actor.kind.value,
                actor_id=actor.subject,
                previous_state="paused",
                new_state=package.status.value,
                payload={"managed_goal_id": str(goal.id), "status_generation": package.version},
            )
        )
    goal.constraints = {**goal.constraints, "paused_children": {}}


async def _control_task(
    session: AsyncSession, task_id: uuid.UUID, actor: Actor, *, stop: bool
) -> None:
    from vuzol.storage.errors import IllegalTransition
    from vuzol.storage.models import Task
    from vuzol.storage.types import USER_TERMINAL_TASK_STATUSES, TaskStatus
    from vuzol.workflows.controls import cancel_task, pause_task
    from vuzol.workflows.transitions import transition_task

    try:
        async with session.begin_nested():
            if stop:
                await cancel_task(session, task_id, actor_id=actor.subject)
            else:
                await pause_task(session, task_id, actor_id=actor.subject)
        return
    except (ValueError, IllegalTransition):
        pass
    task = await session.get(Task, task_id, with_for_update=True)
    if (
        task is None
        or task.status in USER_TERMINAL_TASK_STATUSES
        or (task.status is TaskStatus.PAUSED and not stop)
    ):
        return
    target = TaskStatus.CANCELLED if stop else TaskStatus.PAUSED
    try:
        async with session.begin_nested():
            await transition_task(session, task, target, actor_type="user", actor_id=actor.subject)
    except (ValueError, IllegalTransition):
        return


async def _wait_already_satisfied(session: AsyncSession, condition: Mapping[str, Any]) -> bool:
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


def _uuid(value: uuid.UUID | None) -> str | None:
    return None if value is None else str(value)


def _version_of(result: Mapping[str, Any]) -> int | None:
    version = result.get("version")
    return version if isinstance(version, int) else None
