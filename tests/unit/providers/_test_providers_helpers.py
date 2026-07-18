import hashlib
import json
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from vuzol.config import (
    BudgetMode,
    Capability,
    LaunchMode,
    ProfileRegistry,
    ProviderProfileConfig,
    ProviderRole,
    ScopedSecretResolver,
)
from vuzol.experiments.domain import WorkerEditReport
from vuzol.providers.budgets import account_usage, estimate_reservation
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.domain import (
    ContextItem,
    EffectiveProfileState,
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderRequest,
    QuotaState,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.openai import OpenAICompatibleAdapter
from vuzol.providers.policy import ExclusionReason, RoutingRequest, select_profile
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.providers.registry import AdapterRegistry
from vuzol.routing import select_profile as public_select_profile
from vuzol.workflows.ports import CancellationContext

__all__ = [
    "AdapterRegistry",
    "BudgetMode",
    "CancellationContext",
    "Capability",
    "CodexCliAdapter",
    "CodexInvocation",
    "CodexProcessResult",
    "ContextItem",
    "Decimal",
    "EffectiveProfileState",
    "ExclusionReason",
    "FakeCodexTransport",
    "GrokCliAdapter",
    "LaunchMode",
    "NormalizedUsage",
    "OpenAICompatibleAdapter",
    "Path",
    "ProfileRegistry",
    "ProviderErrorCategory",
    "ProviderFailure",
    "ProviderProfileConfig",
    "ProviderRequest",
    "ProviderRole",
    "QuotaState",
    "RoutingRequest",
    "ScopedSecretResolver",
    "SecretStr",
    "StaticCodexTransport",
    "WorkerEditReport",
    "_grok_diagnostic_events",
    "_grok_tool_updates",
    "account_usage",
    "codex_jsonl",
    "codex_profile",
    "codex_request",
    "dataclass",
    "estimate_reservation",
    "field",
    "hashlib",
    "httpx",
    "json",
    "profile",
    "provider_request",
    "public_select_profile",
    "pytest",
    "routing_request",
    "select_profile",
    "uuid",
]


def profile(profile_id: str, **changes: object) -> ProviderProfileConfig:
    values: dict[str, object] = {
        "id": profile_id,
        "provider": "openai-compatible",
        "model": "model",
        "api_base_url": "https://provider.example/v1",
        "launch_mode": "api",
        "credential_reference": f"env:{profile_id.upper()}_KEY",
        "capabilities": frozenset({Capability.REPOSITORY_READ}),
        "concurrency_limit": 1,
        "cost_class": "balanced",
        "roles": frozenset({ProviderRole.EXECUTOR}),
        "supported_task_types": frozenset({"general"}),
        "sandbox_required": False,
    }
    values.update(changes)
    return ProviderProfileConfig.model_validate(values)


def routing_request(**changes: object) -> RoutingRequest:
    values: dict[str, object] = {
        "role": ProviderRole.EXECUTOR,
        "task_type": "general",
        "required_capabilities": frozenset({Capability.REPOSITORY_READ}),
        "project_allowed_capabilities": frozenset({Capability.REPOSITORY_READ}),
        "budget_mode": BudgetMode.BALANCED,
        "estimated_input_tokens": 100,
        "max_output_tokens": 100,
        "remaining_cost_units": 10.0,
    }
    values.update(changes)
    return RoutingRequest(**values)  # type: ignore[arg-type]


def provider_request(*, structured: bool = False) -> ProviderRequest:
    return ProviderRequest(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        provider_attempt=1,
        role=ProviderRole.EXECUTOR,
        required_capabilities=frozenset(),
        original_input="answer safely",
        task_draft={"task_type": "general"},
        context=(
            ContextItem(
                source="task",
                reference="task:original",
                content="bounded context",
                content_hash=hashlib.sha256(b"bounded context").hexdigest(),
            ),
        ),
        output_schema_name="answer" if structured else None,
        output_schema_version="1" if structured else None,
        output_json_schema={"type": "object"} if structured else None,
        system_policy_revision="policy-v1",
        prompt_revision="prompt-v1",
        timeout_seconds=10,
        max_input_tokens=1_000,
        max_output_tokens=100,
        reserved_cost_units=Decimal("0.1"),
        reserved_quota_units=Decimal("1"),
    )


@dataclass
class FakeCodexTransport:
    invocations: list[CodexInvocation] = field(default_factory=list)

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        assert not cancellation.requested
        self.invocations.append(invocation)
        return CodexProcessResult(
            exit_code=0,
            stdout=json.dumps(
                {
                    "text": "safe result",
                    "request_id": "request",
                    "session_id": "session",
                    "usage": {"input_tokens": 3, "output_tokens": 2},
                }
            ),
            stderr="",
            duration_ms=25,
        )


@dataclass
class StaticCodexTransport:
    stdout: str
    duration_ms: int = 37
    invocations: list[CodexInvocation] = field(default_factory=list)

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        assert not cancellation.requested
        self.invocations.append(invocation)
        return CodexProcessResult(0, self.stdout, "", self.duration_ms)


def codex_jsonl(final_text: str) -> str:
    """Sanitized shape retained from the production Codex MVP session."""
    return "\n".join(
        (
            json.dumps({"type": "thread.started", "thread_id": "session-safe"}),
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "intermediate status"},
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": final_text},
                }
            ),
            json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {
                        "input_tokens": 143,
                        "cached_tokens": 21,
                        "output_tokens": 17,
                    },
                }
            ),
        )
    )


def codex_profile(tmp_path: Path) -> ProviderProfileConfig:
    return profile(
        "codex-a",
        provider="codex",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="codex-a",
        state_directory=tmp_path / "codex-a",
    )


def codex_request(*, schema: dict[str, object] | None = None) -> ProviderRequest:
    request = provider_request(structured=schema is not None).model_copy(
        update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
    )
    return request.model_copy(update={"output_json_schema": schema})


def _grok_diagnostic_events(
    *, decision: str = "allow", completed: bool = True, category: str | None = None
) -> str:
    events: list[dict[str, object]] = [
        {
            "ts": "2026-07-14T02:58:31.700Z",
            "type": "turn_started",
            "schema_version": "1.0",
            "session_id": "019f5e8d-d90b-7e40-a698-8a71fa87eff8",
            "turn_number": 0,
        },
        {
            "ts": "2026-07-14T02:58:31.701Z",
            "type": "tool_started",
            "tool_name": "run_terminal_command",
        },
        {
            "ts": "2026-07-14T02:58:31.702Z",
            "type": "permission_requested",
            "tool_name": "run_terminal_command",
        },
        {
            "ts": "2026-07-14T02:58:31.703Z",
            "type": "permission_resolved",
            "tool_name": "run_terminal_command",
            "decision": decision,
            "wait_ms": 0,
        },
    ]
    if completed:
        events.append(
            {
                "ts": "2026-07-14T02:58:31.704Z",
                "type": "tool_completed",
                "tool_name": "run_terminal_command",
                "outcome": "success",
                "duration_ms": 1,
            }
        )
    if category is not None:
        events.append(
            {
                "ts": "2026-07-14T02:58:31.705Z",
                "type": "turn_ended",
                "outcome": "cancelled",
                "cancellation_category": category,
            }
        )
    return "\n".join(json.dumps(event) for event in events)


def _grok_tool_updates(command: str, *, completed: bool) -> str:
    call_id = "call-1aa3af3d-e549-4c73-ac4e-fc0c08302ed2-31"
    updates: list[dict[str, object]] = [
        {
            "method": "session/update",
            "params": {
                "sessionId": "019f5e8d-d90b-7e40-a698-8a71fa87eff8",
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": call_id,
                    "title": "unsafe display title",
                    "rawInput": {"command": command, "description": "private task detail"},
                    "_meta": {
                        "x.ai/tool": {
                            "name": "run_terminal_command",
                            "kind": "execute",
                            "input": {"command": command},
                        }
                    },
                },
            },
            "timestamp": "2026-07-14T02:58:31.701Z",
        },
        {
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": call_id,
                    "status": "completed" if completed else "failed",
                    "rawOutput": {"stdout": ["private tool output"]},
                }
            },
            "timestamp": "2026-07-14T02:58:31.704Z",
        },
    ]
    return "\n".join(json.dumps(update) for update in updates)
