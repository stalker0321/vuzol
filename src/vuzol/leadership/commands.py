"""Typed project-lead commands and the canonical payload hash."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class LeadCommandName(StrEnum):
    GET_GOAL = "get_goal"
    GET_PROJECT_STATE = "get_project_state"
    GET_CAPABILITIES = "get_capabilities"
    GET_RESULT = "get_result"
    ENABLE_MANAGED_GOAL = "enable_managed_goal"
    SET_WORK_LIMITS = "set_work_limits"
    PROPOSE_BATCH = "propose_batch"
    START_BATCH = "start_batch"
    REQUEST_REVISION = "request_revision"
    REQUEST_PERMISSION = "request_permission"
    WAIT_FOR = "wait_for"
    PROPOSE_COMPLETION = "propose_completion"
    PAUSE_GOAL = "pause_goal"
    RESUME_GOAL = "resume_goal"
    CANCEL_GOAL = "cancel_goal"
    REVOKE_DELEGATION = "revoke_delegation"
    ANSWER_PERMISSION = "answer_permission"
    ACCEPT_CRITERION = "accept_criterion"
    ACCEPT_UNCERTAINTY = "accept_uncertainty"
    ADMIT_CEILING = "admit_ceiling"
    RECONCILE_COST = "reconcile_cost"


class CommandStatus(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    STALE = "stale"
    DENIED = "denied"
    WAITING = "waiting"
    UNSUPPORTED = "unsupported"


READ_COMMANDS = frozenset(
    {
        LeadCommandName.GET_GOAL,
        LeadCommandName.GET_PROJECT_STATE,
        LeadCommandName.GET_CAPABILITIES,
        LeadCommandName.GET_RESULT,
    }
)

OWNER_ONLY_COMMANDS = frozenset(
    {
        LeadCommandName.ENABLE_MANAGED_GOAL,
        LeadCommandName.SET_WORK_LIMITS,
        LeadCommandName.RESUME_GOAL,
        LeadCommandName.REVOKE_DELEGATION,
        LeadCommandName.ACCEPT_CRITERION,
        LeadCommandName.ACCEPT_UNCERTAINTY,
        LeadCommandName.ADMIT_CEILING,
        LeadCommandName.RECONCILE_COST,
    }
)

# Server-defined grant. Callers cannot add to this set.
DELEGATED_ACTIONS = frozenset(
    {
        LeadCommandName.GET_GOAL.value,
        LeadCommandName.GET_PROJECT_STATE.value,
        LeadCommandName.GET_CAPABILITIES.value,
        LeadCommandName.GET_RESULT.value,
        LeadCommandName.PROPOSE_BATCH.value,
        LeadCommandName.START_BATCH.value,
        LeadCommandName.REQUEST_REVISION.value,
        LeadCommandName.REQUEST_PERMISSION.value,
        LeadCommandName.WAIT_FOR.value,
        LeadCommandName.PROPOSE_COMPLETION.value,
        LeadCommandName.PAUSE_GOAL.value,
        LeadCommandName.CANCEL_GOAL.value,
        "apply_integration",
    }
)

PROTECTED_BRANCHES = frozenset(
    {"main", "master", "trunk", "default", "production", "prod", "release"}
)


@dataclass(frozen=True, slots=True)
class LeadCommand:
    name: str
    project_id: str
    idempotency_key: str
    payload: dict[str, Any]
    goal_id: uuid.UUID | None = None
    expected_version: int | None = None
    reason: str = ""
    contract_version: str = "project-lead.v1"

    def canonical_hash(self) -> str:
        return payload_hash(self)


@dataclass(frozen=True, slots=True)
class CommandResult:
    status: str
    actor_kind: str
    actor_subject: str
    command_name: str
    project_id: str
    receipt_id: uuid.UUID | None
    goal_id: uuid.UUID | None
    version: int | None
    reason: str | None
    result: dict[str, Any]


def payload_hash(command: LeadCommand) -> str:
    body = {
        "command": command.name,
        "contract_version": command.contract_version,
        "expected_version": command.expected_version,
        "goal_id": None if command.goal_id is None else str(command.goal_id),
        "payload": command.payload,
        "project_id": command.project_id,
        "reason": command.reason,
    }
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def parse_command_name(name: str) -> LeadCommandName | None:
    try:
        return LeadCommandName(name)
    except ValueError:
        return None
