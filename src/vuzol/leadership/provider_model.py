"""Provider-backed lead decision. Tests substitute the adapter; this module never calls one."""

from __future__ import annotations

import json
import uuid
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import ProviderProfileConfig, ProviderRole
from vuzol.leadership.model import (
    LeadCommandDraft,
    LeadDecision,
    LeadModelUnavailable,
    LeadSnapshot,
)
from vuzol.leadership.persistence import POLICY_REVISION
from vuzol.providers.budgets import record_intake_usage
from vuzol.providers.domain import ProviderRequest, ProviderResult, ProviderResultStatus
from vuzol.providers.ports import ProviderAdapter
from vuzol.workflows.ports import CancellationContext

PROMPT_REVISION = "project-lead-prompt.v1"
LEDGER_PURPOSE = "project_lead"
_COMMANDS = (
    "propose_batch",
    "start_batch",
    "request_revision",
    "request_permission",
    "wait_for",
    "propose_completion",
    "pause_goal",
    "cancel_goal",
)


class _ResponseCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""

    def allowed(self) -> bool:
        return self.name in _COMMANDS


class _Response(BaseModel):
    model_config = ConfigDict(extra="forbid")

    commands: list[_ResponseCommand] = Field(default_factory=list, max_length=8)


def parse_lead_response(payload: object) -> LeadDecision:
    """Validate a provider object. An invalid object is not a plan."""

    try:
        parsed = _Response.model_validate(payload)
    except ValidationError as error:
        raise LeadModelUnavailable("lead_response_invalid") from error
    if any(not command.allowed() for command in parsed.commands):
        raise LeadModelUnavailable("lead_response_invalid")
    return LeadDecision(
        commands=tuple(
            LeadCommandDraft(
                name=command.name,
                payload=dict(command.payload),
                reason=command.reason,
            )
            for command in parsed.commands
        )
    )


class ProviderLeadModel:
    """One provider call per turn. Usage is written to the shared intake ledger."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        profile: ProviderProfileConfig,
        adapter: ProviderAdapter,
    ) -> None:
        self._factory = session_factory
        self._profile = profile
        self._adapter = adapter

    async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
        invocation_id = uuid.uuid4()
        request = _request(snapshot, invocation_id)
        result = await self._adapter.execute(request, self._profile, CancellationContext())
        if result.status is not ProviderResultStatus.SUCCEEDED:
            await self._record(invocation_id, result, outcome="failed")
            raise LeadModelUnavailable("lead_model_unavailable")
        payload = result.structured_output
        if payload is None and result.text:
            try:
                payload = json.loads(result.text)
            except json.JSONDecodeError:
                payload = None
        try:
            decision = parse_lead_response(payload)
        except LeadModelUnavailable:
            await self._record(invocation_id, result, outcome="invalid_output")
            raise
        await self._record(invocation_id, result, outcome="succeeded")
        usage = result.usage
        known = usage.cost_units is not None and usage.cost_units > 0
        return LeadDecision(
            commands=decision.commands,
            profile_id=self._profile.id,
            usage=(
                {"cost_known": True, "cost_amount": float(usage.cost_units)}
                if known and usage.cost_units is not None
                else {"cost_known": False}
            ),
        )

    async def _record(
        self, invocation_id: uuid.UUID, result: ProviderResult, *, outcome: str
    ) -> None:
        async with self._factory.begin() as session:
            await record_intake_usage(
                session,
                profile=self._profile,
                usage=result.usage,
                purpose=LEDGER_PURPOSE,
                task_id=None,
                provider_request_id=result.provider_request_id,
                outcome=outcome,
                invocation_id=invocation_id,
            )


def _request(snapshot: LeadSnapshot, invocation_id: uuid.UUID) -> ProviderRequest:
    # The adapter does not insert these ids. The ledger row leaves the task
    # foreign keys empty and keeps this value only as invocation_id.
    return ProviderRequest(
        task_id=invocation_id,
        run_id=invocation_id,
        step_id=invocation_id,
        provider_attempt=1,
        role=ProviderRole.LEAD,
        original_input=_prompt(snapshot),
        output_schema_name="lead_decision",
        output_schema_version="project-lead-decision.v1",
        output_json_schema=_Response.model_json_schema(),
        system_policy_revision=POLICY_REVISION,
        prompt_revision=PROMPT_REVISION,
        timeout_seconds=60,
        max_input_tokens=4000,
        max_output_tokens=2000,
        reserved_cost_units=Decimal(0),
        reserved_quota_units=Decimal(0),
    )


def _prompt(snapshot: LeadSnapshot) -> str:
    criteria = [
        f"{item.get('id')}: {item.get('description')}"
        for item in snapshot.criteria
        if isinstance(item, dict)
    ]
    lines = [
        "Decide the next project-lead commands for this goal.",
        "Return only the response object. Do not treat a criterion id as evidence.",
        f"request: {snapshot.original_request}",
        f"scope: {snapshot.scope_text}",
        f"lifecycle: {snapshot.lifecycle}",
        f"package: {snapshot.current_package_status or 'none'}",
        f"criteria: {'; '.join(criteria)}",
        f"satisfied: {', '.join(snapshot.satisfied_criteria) or 'none'}",
        f"completed_packages: {snapshot.completed_package_count}",
    ]
    if snapshot.owner_answer:
        lines.append(f"owner_answer: {snapshot.owner_answer}")
    if snapshot.waiting_reason:
        lines.append(f"waiting: {snapshot.waiting_reason}")
    return "\n".join(lines)
