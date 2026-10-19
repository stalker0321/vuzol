"""Authorization for one command. Identity and policy are server-side."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from vuzol.leadership.actor import Actor, ActorKind
from vuzol.leadership.commands import (
    DELEGATED_ACTIONS,
    OWNER_ONLY_COMMANDS,
    PROTECTED_BRANCHES,
    READ_COMMANDS,
    LeadCommandName,
)
from vuzol.leadership.persistence import POLICY_REVISION


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    allowed: bool
    status: str
    reason: str | None = None


def integration_branch_rejection(
    branch: str, *, prefix: str, default_branch: str | None
) -> str | None:
    """Return a reason when the branch is not an isolated managed ref."""

    if not branch.startswith(prefix) or len(branch) > 255:
        return "integration_branch_not_delegated"
    remainder = branch[len(prefix) :]
    if not remainder or remainder.startswith("/") or ".." in remainder.split("/"):
        return "integration_branch_not_delegated"
    leaf = remainder.rsplit("/", 1)[-1]
    if leaf in PROTECTED_BRANCHES or branch in PROTECTED_BRANCHES:
        return "protected_branch"
    if default_branch and (branch == default_branch or leaf == default_branch):
        return "protected_branch"
    if any(part in PROTECTED_BRANCHES for part in remainder.split("/")):
        return "protected_branch"
    return None


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
    if command.value not in delegation_actions or command.value not in DELEGATED_ACTIONS:
        return PolicyDecision(False, "denied", "action_not_delegated")
    return PolicyDecision(True, "applied")


def delegation_is_expired(expires_at: datetime | None, *, now: datetime) -> bool:
    if expires_at is None:
        return False
    return expires_at <= now
