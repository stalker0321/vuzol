"""Project-lead model port. Production ships an unconfigured model, not a paid call."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol


class LeadModelUnavailable(RuntimeError):
    """The lead has no model. Callers must not invent a plan."""


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
    original_request: str
    scope_text: str
    waiting_reason: str | None


@dataclass(frozen=True, slots=True)
class LeadCommandDraft:
    name: str
    payload: dict[str, Any]
    reason: str = ""


@dataclass(frozen=True, slots=True)
class LeadDecision:
    commands: tuple[LeadCommandDraft, ...] = ()
    profile_id: str = "unconfigured"
    usage: dict[str, Any] = field(default_factory=lambda: {"cost_known": False})


class LeadModel(Protocol):
    async def decide(self, snapshot: LeadSnapshot) -> LeadDecision: ...


class UnconfiguredLeadModel:
    """Records that no model is attached. It does not propose work."""

    async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
        del snapshot
        raise LeadModelUnavailable("lead_model_unconfigured")


def normalize_usage(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Unknown cost stays unknown. Zero is not a measured amount."""

    if not isinstance(raw, Mapping) or raw.get("cost_known") is not True:
        return {"cost_known": False}
    amount = raw.get("cost_amount")
    if isinstance(amount, bool) or not isinstance(amount, int | float) or amount <= 0:
        return {"cost_known": False}
    return {"cost_known": True, "cost_amount": amount}


def decision_payload(decision: LeadDecision) -> dict[str, Any]:
    usage = normalize_usage(decision.usage)
    return {
        "commands": [
            {"name": command.name, "payload": dict(command.payload), "reason": command.reason}
            for command in decision.commands
        ],
        "profile_id": decision.profile_id,
        "usage": usage,
    }


def decision_from_payload(raw: Mapping[str, Any] | None) -> LeadDecision:
    if not isinstance(raw, Mapping):
        return LeadDecision()
    commands_raw = raw.get("commands")
    commands: list[LeadCommandDraft] = []
    if isinstance(commands_raw, list):
        for item in commands_raw:
            if not isinstance(item, dict) or not item.get("name"):
                continue
            payload = item.get("payload")
            commands.append(
                LeadCommandDraft(
                    name=str(item["name"]),
                    payload=dict(payload) if isinstance(payload, dict) else {},
                    reason=str(item.get("reason") or ""),
                )
            )
    usage = raw.get("usage")
    return LeadDecision(
        commands=tuple(commands),
        profile_id=str(raw.get("profile_id") or "unconfigured"),
        usage=dict(usage) if isinstance(usage, dict) else {"cost_known": False},
    )
