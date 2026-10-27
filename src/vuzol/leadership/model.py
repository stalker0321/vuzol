"""Project-lead model port. A missing profile parks the goal and does not call a provider."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, TypedDict

from vuzol.leadership.commands import MAX_MEMORY_COMMANDS, MEMORY_COMMANDS


class NotebookNote(TypedDict):
    id: str
    text: str
    created_at: str


class LeadModelUnavailable(RuntimeError):
    """The lead has no model. Callers must not invent a plan."""

    def __init__(self, reason: str, *, retryable: bool = True) -> None:
        super().__init__(reason)
        # False when the provider said a resend cannot help (auth, a bad request).
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class LeadSnapshot:
    goal_id: uuid.UUID
    project_id: str
    lifecycle: str
    version: int
    fence_epoch: int
    scope_revision: int
    criteria: tuple[dict[str, Any], ...]
    satisfied_criteria: tuple[str, ...]
    integration_branch: str
    current_package_id: str | None
    current_package_status: str | None
    integration_head_commit: str | None
    request: str
    scope_text: str
    waiting_reason: str | None
    completed_package_count: int = 0
    owner_answer: str | None = None
    owner_event: dict[str, Any] | None = None
    superseded_request: str | None = None
    invocation_id: uuid.UUID | None = None
    result_context: dict[str, Any] = field(default_factory=dict)
    project_notebook: tuple[NotebookNote, ...] = ()
    global_notebook: tuple[NotebookNote, ...] = ()


@dataclass(frozen=True, slots=True)
class LeadCommandDraft:
    name: str
    payload: dict[str, Any]
    reason: str = ""


@dataclass(frozen=True, slots=True)
class LeadDecision:
    commands: tuple[LeadCommandDraft, ...] = ()
    reply: str = ""
    profile_id: str = "unconfigured"
    usage: dict[str, Any] = field(default_factory=lambda: {"cost_known": False})
    # Server-owned planner outcome, never parsed from lead model output.
    planning_outcome: dict[str, Any] | None = None


class LeadModel(Protocol):
    async def decide(self, snapshot: LeadSnapshot) -> LeadDecision: ...


def normalize_usage(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Unknown cost stays unknown. Zero is not a measured amount."""

    if not isinstance(raw, Mapping) or raw.get("cost_known") is not True:
        return {"cost_known": False}
    amount = raw.get("cost_amount")
    if isinstance(amount, bool) or not isinstance(amount, int | float) or amount <= 0:
        return {"cost_known": False}
    return {"cost_known": True, "cost_amount": amount}


def freeze_decision(
    decision: LeadDecision,
    *,
    goal_id: uuid.UUID,
    wakeup_id: uuid.UUID,
    input_version: int,
    input_fence: int,
    input_scope: int,
    input_lease_epoch: int,
) -> dict[str, Any]:
    """Immutable command list. Keys and expected versions do not follow later state."""

    if sum(command.name in MEMORY_COMMANDS for command in decision.commands) > MAX_MEMORY_COMMANDS:
        raise LeadModelUnavailable("lead_response_invalid")
    usage = normalize_usage(decision.usage)
    commands = []
    for ordinal, command in enumerate(decision.commands):
        commands.append(
            {
                "ordinal": ordinal,
                "name": command.name,
                "payload": dict(command.payload),
                "reason": command.reason,
                "idempotency_key": f"lead:{goal_id}:{wakeup_id}:{ordinal}",
                "expected_version": input_version + ordinal,
            }
        )
    return {
        "commands": commands,
        "reply": decision.reply,
        "profile_id": decision.profile_id,
        "usage": usage,
        "input_version": input_version,
        "input_fence": input_fence,
        "input_scope": input_scope,
        "input_lease_epoch": input_lease_epoch,
        "planning_outcome": decision.planning_outcome,
    }
