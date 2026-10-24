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
    REQUEST_PLAN = "request_plan"
    REVISE_PLAN = "revise_plan"
    START_BATCH = "start_batch"
    START_TASK = "start_task"
    CANCEL_TASK = "cancel_task"
    ACCEPT_RESULT = "accept_result"
    AMEND_GOAL = "amend_goal"
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
        LeadCommandName.REQUEST_PLAN.value,
        LeadCommandName.REVISE_PLAN.value,
        LeadCommandName.START_BATCH.value,
        LeadCommandName.START_TASK.value,
        LeadCommandName.CANCEL_TASK.value,
        LeadCommandName.ACCEPT_RESULT.value,
        LeadCommandName.REQUEST_REVISION.value,
        LeadCommandName.AMEND_GOAL.value,
        LeadCommandName.REQUEST_PERMISSION.value,
        LeadCommandName.WAIT_FOR.value,
        LeadCommandName.PROPOSE_COMPLETION.value,
        LeadCommandName.PAUSE_GOAL.value,
        LeadCommandName.CANCEL_GOAL.value,
        "apply_integration",
    }
)

# A conversation may plan and clarify, but only the owner's plan approval grants execution.
DRAFT_ACTIONS = frozenset(c.value for c in READ_COMMANDS) | frozenset(
    {
        "request_plan",
        "revise_plan",
        "amend_goal",
        "request_permission",
        "pause_goal",
        "cancel_goal",
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
    # Provenance describes the first accepted ingress, not the work's semantic
    # identity. Keep retries compatible with older receipts (before provenance)
    # and with a renamed OAuth client. Different work still conflicts.
    payload = (
        {key: value for key, value in command.payload.items() if key != "request_origin"}
        if command.name == LeadCommandName.ENABLE_MANAGED_GOAL.value
        else command.payload
    )
    body = {
        # Preserve hashes of receipts issued under the original input name.
        "command": "request_revision" if command.name == "amend_goal" else command.name,
        "contract_version": command.contract_version,
        "expected_version": command.expected_version,
        "goal_id": None if command.goal_id is None else str(command.goal_id),
        "payload": payload,
        "project_id": command.project_id,
        "reason": command.reason,
    }
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def parse_command_name(name: str) -> LeadCommandName | None:
    try:
        return LeadCommandName(
            {
                "request_revision": "amend_goal",
                "ask_owner": "request_permission",
                "read_status": "get_project_state",
                "propose_plan": "request_plan",
                "propose_batch": "request_plan",
            }.get(name, name)
        )
    except ValueError:
        return None
