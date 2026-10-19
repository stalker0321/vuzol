"""Single command boundary for human and agent project-lead callers."""

from __future__ import annotations

import re
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.discussion.domain import DomainError, PlanDraft, PlanItemDraft
from vuzol.leadership.actor import Actor, ActorKind, ActorRejected
from vuzol.leadership.commands import (
    DELEGATED_ACTIONS,
    READ_COMMANDS,
    CommandResult,
    CommandStatus,
    LeadCommand,
    LeadCommandName,
    parse_command_name,
    payload_hash,
)
from vuzol.leadership.evidence import assess_completion
from vuzol.leadership.persistence import (
    CONTRACT_VERSION,
    LIVE_GOAL_LIFECYCLES,
    POLICY_REVISION,
    GoalDelegation,
    LeadershipCommandReceipt,
    ManagedGoal,
)
from vuzol.leadership.policy import (
    PolicyDecision,
    authorize,
    delegation_is_expired,
    integration_branch_rejection,
)
from vuzol.leadership.wakeups import enabled_wakeup_key, enqueue_goal_wakeup
from vuzol.storage.models import Event, WorkPackage
from vuzol.storage.types import EstimatedComplexity, RiskLevel, WorkPackageStatus
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
    ) -> None:
        self._factory = session_factory
        self._enabled = enabled
        self._branch_prefix = branch_prefix
        self._repeat_limit = repeat_limit
        self._default_branches = dict(default_branches or {})
        self._batch_executor = batch_executor

    async def apply(self, actor: Actor, command: LeadCommand) -> CommandResult:
        name = parse_command_name(command.name)
        digest = payload_hash(command)
        async with UnitOfWork(self._factory) as uow:
            assert uow.session is not None
            existing = await _receipt(uow.session, actor, command.idempotency_key)
            if existing is not None:
                return _duplicate_or_conflict(existing, actor, command, digest)
            if name is None or command.contract_version != CONTRACT_VERSION:
                return await _store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.UNSUPPORTED, "unsupported_command", {}, None),
                )
            if _FORBIDDEN_PAYLOAD_KEYS.intersection(command.payload):
                return await _store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.DENIED, "caller_supplied_identity", {}, None),
                )
            goal = await _lock_goal(uow.session, command)
            delegation = (
                None
                if goal is None
                else await uow.session.get(GoalDelegation, goal.delegation_id, with_for_update=True)
            )
            decision = _decision(actor, name, command, goal, delegation, self._enabled)
            if not decision.allowed:
                return await _store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus(decision.status), decision.reason, {}, goal),
                )
            version_problem = _version_problem(name, command, goal)
            if version_problem is not None:
                return await _store(
                    uow.session,
                    actor,
                    command,
                    digest,
                    _Outcome(CommandStatus.STALE, version_problem, {}, goal),
                )
            try:
                outcome = await self._perform(uow, actor, name, command, goal, delegation)
            except DomainError as error:
                outcome = _Outcome(CommandStatus.DENIED, error.code, {}, goal)
            return await _store(uow.session, actor, command, digest, outcome)

    async def apply_human(self, principal: Principal, command: LeadCommand) -> CommandResult:
        return await self.apply(Actor.from_principal(principal), command)

    async def apply_agent(
        self,
        agent_principal_id: str,
        ingress_source: str,
        command: LeadCommand,
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
        return await self.apply(actor, command)

    async def _perform(
        self,
        uow: UnitOfWork,
        actor: Actor,
        name: LeadCommandName,
        command: LeadCommand,
        goal: ManagedGoal | None,
        delegation: GoalDelegation | None,
    ) -> _Outcome:
        assert uow.session is not None
        if name is LeadCommandName.ENABLE_MANAGED_GOAL:
            return await self._enable(uow, actor, command)
        if goal is None or delegation is None:
            return _Outcome(CommandStatus.DENIED, "goal_not_found", {}, None)
        if goal.lifecycle in _TERMINAL and name not in READ_COMMANDS:
            return _Outcome(CommandStatus.DENIED, "goal_terminal", {}, goal)
        if goal.lifecycle == "paused" and name not in READ_COMMANDS | {
            LeadCommandName.RESUME_GOAL,
            LeadCommandName.CANCEL_GOAL,
            LeadCommandName.REVOKE_DELEGATION,
        }:
            return _Outcome(CommandStatus.DENIED, "goal_paused", {}, goal)
        if name in READ_COMMANDS:
            return _Outcome(CommandStatus.APPLIED, None, _view(name, goal, delegation), goal)
        if name is LeadCommandName.PAUSE_GOAL:
            return _transition(goal, actor, "paused", "owner_pause", CommandStatus.APPLIED)
        if name is LeadCommandName.RESUME_GOAL:
            if goal.lifecycle != "paused":
                return _Outcome(CommandStatus.DENIED, "goal_not_paused", {}, goal)
            return _transition(goal, actor, "active", None, CommandStatus.APPLIED)
        if name is LeadCommandName.CANCEL_GOAL:
            return _transition(goal, actor, "cancelled", "cancelled", CommandStatus.APPLIED)
        if name is LeadCommandName.REVOKE_DELEGATION:
            delegation.revoked_at = datetime.now(UTC)
            delegation.version += 1
            return _transition(goal, actor, "paused", "delegation_revoked", CommandStatus.APPLIED)
        if name is LeadCommandName.REQUEST_PERMISSION:
            question = str(command.payload.get("question") or "").strip()
            if not question:
                return _Outcome(CommandStatus.DENIED, "question_required", {}, goal)
            goal.wakeup_condition = {"kind": "owner", "question": question}
            return _transition(goal, actor, "waiting", "owner", CommandStatus.WAITING)
        if name is LeadCommandName.WAIT_FOR:
            condition = command.payload.get("condition")
            if not isinstance(condition, dict) or not condition.get("kind"):
                return _Outcome(CommandStatus.DENIED, "wait_condition_required", {}, goal)
            if await _wait_already_satisfied(uow.session, condition):
                return _Outcome(
                    CommandStatus.APPLIED,
                    "already_satisfied",
                    {"version": goal.version},
                    goal,
                )
            goal.wakeup_condition = dict(condition)
            goal.lease_owner = None
            goal.lease_until = None
            return _transition(goal, actor, "waiting", "wait_for", CommandStatus.WAITING)
        if name is LeadCommandName.PROPOSE_BATCH:
            plan = _plan_draft(command.payload)
            goal.proposed_plan = command.payload["plan"]
            _bump(goal, actor)
            return _Outcome(
                CommandStatus.APPLIED,
                None,
                {"version": goal.version, "title": plan.title, "items": len(plan.items)},
                goal,
            )
        if name is LeadCommandName.START_BATCH:
            if self._batch_executor is None:
                return _Outcome(CommandStatus.UNSUPPORTED, "batch_executor_unbound", {}, goal)
            try:
                async with uow.session.begin_nested():
                    produced = await self._batch_executor(
                        uow,
                        actor=actor,
                        goal=goal,
                        delegation=delegation,
                        command=command,
                        repeat_limit=self._repeat_limit,
                    )
            except DomainError as error:
                return _Outcome(CommandStatus.DENIED, error.code, {}, goal)
            status = CommandStatus(str(produced.pop("status", CommandStatus.APPLIED.value)))
            reason = produced.pop("reason", None)
            return _Outcome(status, None if reason is None else str(reason), produced, goal)
        if name is LeadCommandName.REQUEST_REVISION:
            goal.wakeup_condition = {"kind": "revision", "note": command.reason}
            return _transition(goal, actor, "waiting", "revision", CommandStatus.WAITING)
        if name is LeadCommandName.PROPOSE_COMPLETION:
            assessment = await assess_completion(uow.session, goal)
            if assessment.satisfied:
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
            goal.wakeup_condition = {"kind": "owner", "missing": list(assessment.missing)}
            return _transition(
                goal, actor, "waiting", "evidence_not_recorded", CommandStatus.WAITING
            )
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
        principal = f"agent:project-lead:{command.project_id}"
        delegation = GoalDelegation(
            project_id=command.project_id,
            owner_user_id=actor.user_id,
            agent_principal_id=principal,
            policy_revision=POLICY_REVISION,
            allowed_actions=sorted(DELEGATED_ACTIONS),
            integration_branch=branch,
            environments=["isolated"],
        )
        uow.session.add(delegation)
        await uow.session.flush()
        default_branch = self._default_branches.get(command.project_id)
        constraints: dict[str, Any] = {}
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


def _view(name: LeadCommandName, goal: ManagedGoal, delegation: GoalDelegation) -> dict[str, Any]:
    if name is LeadCommandName.GET_CAPABILITIES:
        return {
            "actions": list(delegation.allowed_actions),
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
    items: list[PlanItemDraft] = []
    for item in items_raw:
        if not isinstance(item, dict):
            raise DomainError("invalid_plan")
        criteria = item.get("completion_criteria")
        if not isinstance(criteria, list):
            raise DomainError("invalid_plan")
        items.append(
            PlanItemDraft(
                summary=str(item.get("summary") or ""),
                goal=str(item.get("goal") or ""),
                expected_outcome=str(item.get("expected_outcome") or ""),
                completion_criteria=tuple(str(value) for value in criteria),
                allowed_scope=str(item.get("allowed_scope") or ""),
                local_id=None if item.get("local_id") is None else str(item.get("local_id")),
                suggested_risk=RiskLevel.LOW,
                estimated_complexity=EstimatedComplexity.SMALL,
            )
        )
    return PlanDraft(title=str(raw.get("title") or ""), items=tuple(items))


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
    if existing.payload_hash != digest or existing.command_name != command.name:
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
    if outcome.goal is not None:
        _event(
            session,
            outcome.goal,
            actor,
            f"managed_goal.command.{outcome.status.value}",
            None,
            outcome.goal.lifecycle,
            extra={"command": command.name, "reason": outcome.reason},
        )
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
    return package.status in {
        WorkPackageStatus.COMPLETED,
        WorkPackageStatus.STOPPED,
        WorkPackageStatus.DISCARDED,
    }


def _uuid(value: uuid.UUID | None) -> str | None:
    return None if value is None else str(value)


def _version_of(result: Mapping[str, Any]) -> int | None:
    version = result.get("version")
    return version if isinstance(version, int) else None
