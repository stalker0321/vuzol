"""Authorization for one command. Identity and policy are server-side."""

from __future__ import annotations

from dataclasses import dataclass

from vuzol.leadership.actor import Actor, ActorKind
from vuzol.leadership.commands import (
    DELEGATED_ACTIONS,
    OWNER_ONLY_COMMANDS,
    READ_COMMANDS,
    LeadCommandName,
)
from vuzol.leadership.persistence import POLICY_REVISION
from vuzol.leadership.rules import delegation_is_expired, integration_branch_rejection

__all__ = ["PolicyDecision", "authorize", "delegation_is_expired", "integration_branch_rejection"]


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    status: str
    reason: str | None = None


def authorize(
    actor: Actor,
    command: LeadCommandName,
    *,
    feature_enabled: bool,
    project_id: str,
    goal_project_id: str | None,
    goal_owner_user_id: int | None,
    delegation_project_id: str | None,
    delegation_agent_principal_id: str | None,
    delegation_actions: frozenset[str],
    delegation_revoked: bool,
    delegation_expired: bool,
    delegation_policy_revision: str | None,
) -> PolicyDecision:
    owner_keeps = READ_COMMANDS | {
        LeadCommandName.PAUSE_GOAL,
        LeadCommandName.CANCEL_GOAL,
        LeadCommandName.REVOKE_DELEGATION,
        LeadCommandName.ANSWER_PERMISSION,
    }
    if not feature_enabled and (actor.kind is ActorKind.AGENT or command not in owner_keeps):
        # New agent decisions stop. The owner can still read, pause, cancel,
        # or revoke a goal that already exists.
        return PolicyDecision(False, "denied", "feature_disabled")

    if command in OWNER_ONLY_COMMANDS:
        if actor.kind is not ActorKind.HUMAN:
            return PolicyDecision(False, "denied", "owner_decision_required")
        if (
            goal_owner_user_id is not None
            and actor.user_id != goal_owner_user_id
            and command is not LeadCommandName.ENABLE_MANAGED_GOAL
        ):
            return PolicyDecision(False, "denied", "outside_scope")
        return PolicyDecision(True, "applied")

    if actor.kind is ActorKind.HUMAN:
        if goal_project_id is not None and goal_project_id != project_id:
            return PolicyDecision(False, "denied", "outside_scope")
        if goal_owner_user_id is not None and actor.user_id != goal_owner_user_id:
            return PolicyDecision(False, "denied", "outside_scope")
        return PolicyDecision(True, "applied")

    if delegation_agent_principal_id != actor.subject:
        return PolicyDecision(False, "denied", "delegation_mismatch")
    if delegation_project_id != project_id or (
        goal_project_id is not None and goal_project_id != project_id
    ):
        return PolicyDecision(False, "denied", "outside_scope")
    if delegation_revoked:
        return PolicyDecision(False, "denied", "delegation_revoked")
    if delegation_expired:
        return PolicyDecision(False, "denied", "delegation_expired")
    if delegation_policy_revision != POLICY_REVISION:
        return PolicyDecision(False, "denied", "policy_revision_mismatch")
    delegation_actions = delegation_actions | frozenset(
        {
            "propose_plan": "propose_batch",
            "ask_owner": "request_permission",
            "read_status": "get_project_state",
        }.get(value, value)
        for value in delegation_actions
    )
    action = command.value
    if command is LeadCommandName.AMEND_GOAL and "request_revision" in delegation_actions:
        action = "request_revision"
    equivalent = {
        "start_task": "start_batch",
        "cancel_task": "cancel_goal",
        "accept_result": "apply_integration",
    }
    if action not in delegation_actions and action in equivalent:
        action = equivalent[action]
    if action not in delegation_actions or action not in DELEGATED_ACTIONS:
        return PolicyDecision(False, "denied", "action_not_delegated")
    return PolicyDecision(True, "applied")
