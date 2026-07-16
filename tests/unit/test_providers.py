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


def test_policy_filters_and_orders_deterministically() -> None:
    cheap = profile("cheap", cost_class="cheap", routing_priority=20)
    preferred = profile("preferred", cost_class="balanced", routing_priority=10)
    saturated = profile("saturated", routing_priority=1)
    unhealthy = profile("unhealthy", routing_priority=0)
    states = {
        "cheap": EffectiveProfileState(),
        "preferred": EffectiveProfileState(),
        "saturated": EffectiveProfileState(active_leases=1),
        "unhealthy": EffectiveProfileState(healthy=False),
    }

    decision = select_profile(routing_request(), (unhealthy, saturated, cheap, preferred), states)

    assert decision.selected_profile_id == "preferred"
    assert decision.alternatives == ("cheap",)
    reasons = {item.profile_id: item.reasons for item in decision.evaluations}
    assert ExclusionReason.CONCURRENCY in reasons["saturated"]
    assert ExclusionReason.UNHEALTHY in reasons["unhealthy"]
    assert decision == select_profile(
        routing_request(), (preferred, cheap, saturated, unhealthy), states
    )


def test_code_execution_requires_cli_sandbox_profile() -> None:
    api = profile("api", sandbox_required=True)
    cli = profile(
        "cli",
        provider="codex",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="cli",
        state_directory=Path("/var/lib/codex-cli"),
        sandbox_required=True,
    )
    decision = select_profile(
        routing_request(requires_sandbox=True, required_launch_mode=LaunchMode.CLI),
        (api, cli),
        {"api": EffectiveProfileState(), "cli": EffectiveProfileState()},
    )
    assert decision.selected_profile_id == "cli"
    api_evaluation = next(item for item in decision.evaluations if item.profile_id == "api")
    assert ExclusionReason.LAUNCH_MODE in api_evaluation.reasons


def test_policy_honors_only_eligible_explicit_profile_and_fallback() -> None:
    primary = profile("primary", fallback_profile_ids=("fallback",))
    fallback = profile("fallback", routing_priority=999)
    forbidden = profile("forbidden", capabilities=frozenset())
    states = {item.id: EffectiveProfileState() for item in (primary, fallback, forbidden)}

    explicit = select_profile(
        routing_request(trusted_profile_id="forbidden"),
        (primary, fallback, forbidden),
        states,
    )
    assert explicit.selected_profile_id == "primary"
    fallback_decision = select_profile(
        routing_request(
            failed_profile_id="primary",
            allowed_fallback_ids=("fallback",),
        ),
        (primary, fallback, forbidden),
        states,
    )
    assert fallback_decision.selected_profile_id == "fallback"
    assert all(
        item.profile_id != "primary" or not item.eligible for item in fallback_decision.evaluations
    )


def test_policy_treats_quota_and_unknown_cost_conservatively() -> None:
    configured = profile("profile", minimum_unknown_usage_cost=0.5)
    quota = select_profile(
        routing_request(),
        (configured,),
        {"profile": EffectiveProfileState(quota_state=QuotaState.EXHAUSTED)},
    )
    assert quota.selected_profile_id is None
    budget = select_profile(
        routing_request(remaining_cost_units=0.1),
        (configured,),
        {"profile": EffectiveProfileState()},
    )
    assert budget.selected_profile_id is None
    estimate = estimate_reservation(configured, input_tokens=1, output_tokens=1)
    assert estimate.cost_units == Decimal("0.500000")


def test_policy_reports_every_static_security_exclusion() -> None:
    incompatible = profile(
        "incompatible",
        roles=frozenset({ProviderRole.PLANNER}),
        supported_task_types=frozenset({"coding"}),
        capabilities=frozenset(),
        sandbox_required=False,
        context_limit=10,
        output_limit=10,
    )
    request = routing_request(
        project_allowed_capabilities=frozenset(),
        estimated_input_tokens=100,
        max_output_tokens=100,
        requires_sandbox=True,
    )
    decision = public_select_profile(
        request, (incompatible,), {"incompatible": EffectiveProfileState()}
    )
    reasons = set(decision.evaluations[0].reasons)
    assert {
        ExclusionReason.ROLE,
        ExclusionReason.TASK_TYPE,
        ExclusionReason.CAPABILITY,
        ExclusionReason.PROJECT_POLICY,
        ExclusionReason.SANDBOX,
        ExclusionReason.CONTEXT_LIMIT,
        ExclusionReason.OUTPUT_LIMIT,
    }.issubset(reasons)


def test_usage_accounting_uses_configured_rates_and_quota() -> None:
    configured = profile(
        "priced",
        input_cost_units_per_million=2,
        output_cost_units_per_million=4,
        quota_units_per_call=3,
    )
    usage = account_usage(
        configured,
        NormalizedUsage(input_tokens=1_000, output_tokens=500, duration_ms=10),
    )
    assert usage.cost_units == Decimal("0.004000")
    assert usage.quota_units == Decimal("3.000000")

    supplied = account_usage(
        configured,
        NormalizedUsage(
            input_tokens=1,
            output_tokens=1,
            cost_units=Decimal("9"),
            quota_units=Decimal("8"),
            duration_ms=1,
        ),
    )
    assert supplied.cost_units == Decimal("9")
    assert supplied.quota_units == Decimal("8")


@pytest.mark.anyio
async def test_openai_adapter_normalizes_structured_result_and_usage() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer test-key"
        payload = json.loads(request.content)
        assert payload["model"] == "model"
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {"content": '{"answer":"ok"}'},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 4,
                    "prompt_tokens_details": {"cached_tokens": 2},
                },
            },
            headers={"x-request-id": "request-1"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="https://provider.example/v1"
    ) as client:
        adapter = OpenAICompatibleAdapter(credential=SecretStr("test-key"), client=client)
        result = await adapter.execute(
            provider_request(structured=True), profile("profile"), CancellationContext()
        )

    assert result.structured_output == {"answer": "ok"}
    assert result.provider_request_id == "request-1"
    assert result.usage.input_tokens == 12
    assert result.usage.cached_tokens == 2


@pytest.mark.anyio
async def test_openai_adapter_uses_gpt5_chat_completion_parameters() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["model"] == "gpt-5-nano-2025-08-07"
        assert payload["max_completion_tokens"] == 1_000
        assert payload["reasoning_effort"] == "minimal"
        assert "max_tokens" not in payload
        assert "temperature" not in payload
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "plan"}, "finish_reason": "stop"}]},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="https://api.openai.com/v1"
    ) as client:
        adapter = OpenAICompatibleAdapter(credential=SecretStr("test-key"), client=client)
        request = provider_request().model_copy(update={"max_output_tokens": 1_000})
        selected = profile("profile").model_copy(update={"model": "gpt-5-nano-2025-08-07"})
        result = await adapter.execute(request, selected, CancellationContext())

    assert result.text == "plan"


@pytest.mark.anyio
async def test_openai_adapter_maps_errors_without_response_body() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(401, text="private provider response")
        ),
        base_url="https://provider.example/v1",
    ) as client:
        adapter = OpenAICompatibleAdapter(credential=SecretStr("test-key"), client=client)
        with pytest.raises(ProviderFailure) as captured:
            await adapter.execute(provider_request(), profile("profile"), CancellationContext())

    assert captured.value.category is ProviderErrorCategory.AUTHENTICATION
    assert "private provider response" not in str(captured.value)


@pytest.mark.anyio
async def test_openai_adapter_rejects_unsafe_requests_before_send() -> None:
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="https://provider.example/v1"
    ) as client:
        adapter = OpenAICompatibleAdapter(credential=SecretStr("key"), client=client)
        cancelled = CancellationContext()
        cancelled.request()
        with pytest.raises(ProviderFailure) as captured:
            await adapter.execute(provider_request(), profile("profile"), cancelled)
        assert captured.value.category is ProviderErrorCategory.CANCELLED
        assert not captured.value.request_sent

        with pytest.raises(ProviderFailure, match="sandbox"):
            await adapter.execute(
                provider_request().model_copy(update={"sandbox_reference": "sandbox:1"}),
                profile("profile"),
                CancellationContext(),
            )
        with pytest.raises(ProviderFailure, match="schema"):
            await adapter.execute(
                provider_request(structured=True).model_copy(
                    update={"output_json_schema": {"type": "not-a-json-schema-type"}}
                ),
                profile("profile"),
                CancellationContext(),
            )
    assert calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("status", "category", "retryable"),
    [
        (403, ProviderErrorCategory.AUTHENTICATION, False),
        (429, ProviderErrorCategory.RATE_LIMITED, True),
        (408, ProviderErrorCategory.TIMEOUT, True),
        (503, ProviderErrorCategory.PROVIDER_UNAVAILABLE, True),
        (413, ProviderErrorCategory.CONTEXT_TOO_LARGE, False),
        (400, ProviderErrorCategory.PERMANENT_REQUEST, False),
    ],
)
async def test_openai_adapter_normalizes_http_categories(
    status: int, category: ProviderErrorCategory, retryable: bool
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(status, headers={"retry-after": "12"})
        ),
        base_url="https://provider.example/v1",
    ) as client:
        adapter = OpenAICompatibleAdapter(credential=SecretStr("key"), client=client)
        with pytest.raises(ProviderFailure) as captured:
            await adapter.execute(provider_request(), profile("profile"), CancellationContext())
    assert captured.value.category is category
    assert captured.value.retryable is retryable
    assert captured.value.request_sent
    assert captured.value.retry_after_seconds == 12


@pytest.mark.anyio
@pytest.mark.parametrize(
    "content",
    ["not-json", "[]", '{"wrong":true}'],
)
async def test_openai_adapter_rejects_invalid_structured_output(content: str) -> None:
    required_schema = {
        "type": "object",
        "required": ["answer"],
        "properties": {"answer": {"type": "string"}},
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200, json={"choices": [{"message": {"content": content}}]}
            )
        ),
        base_url="https://provider.example/v1",
    ) as client:
        adapter = OpenAICompatibleAdapter(credential=SecretStr("key"), client=client)
        request = provider_request(structured=True).model_copy(
            update={"output_json_schema": required_schema}
        )
        with pytest.raises(ProviderFailure) as captured:
            await adapter.execute(request, profile("profile"), CancellationContext())
    assert captured.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT


@pytest.mark.anyio
async def test_openai_adapter_normalizes_timeout_and_invalid_shape() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: (_ for _ in ()).throw(httpx.ReadTimeout("timeout", request=request))
        ),
        base_url="https://provider.example/v1",
    ) as client:
        with pytest.raises(ProviderFailure) as captured:
            await OpenAICompatibleAdapter(credential=SecretStr("key"), client=client).execute(
                provider_request(), profile("profile"), CancellationContext()
            )
    assert captured.value.category is ProviderErrorCategory.TIMEOUT

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={})),
        base_url="https://provider.example/v1",
    ) as client:
        with pytest.raises(ProviderFailure) as captured:
            await OpenAICompatibleAdapter(credential=SecretStr("key"), client=client).execute(
                provider_request(), profile("profile"), CancellationContext()
            )
    assert captured.value.category is ProviderErrorCategory.UNKNOWN


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


@pytest.mark.anyio
async def test_codex_adapter_uses_isolated_identity_without_auth_copy(tmp_path: Path) -> None:
    transport = FakeCodexTransport()
    configured = profile(
        "codex-a",
        provider="codex",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="vuzol-codex-a",
        state_directory=tmp_path / "codex-a",
    )

    result = await CodexCliAdapter(transport).execute(
        provider_request().model_copy(
            update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
        ),
        configured,
        CancellationContext(),
    )

    assert result.text == "safe result"
    invocation = transport.invocations[0]
    assert invocation.runtime_identity == "vuzol-codex-a"
    assert invocation.state_directory == str(tmp_path / "codex-a")
    assert "auth.json" not in invocation.stdin
    assert "--sandbox" not in invocation.argv
    assert "--ignore-rules" in invocation.argv
    assert 'approval_policy="never"' in invocation.argv
    assert 'default_permissions="vuzol-reader"' in invocation.argv
    assert '"/workspace"="read"' in " ".join(invocation.argv)
    assert '"/artifacts"' not in " ".join(invocation.argv)
    assert '"/codex-home"="none"' in " ".join(invocation.argv)
    assert "network={enabled=false}" in " ".join(invocation.argv)
    assert invocation.argv[-3:] == ("--cd", "/workspace", "-")


@pytest.mark.anyio
async def test_codex_adapter_accepts_versioned_jsonl(tmp_path: Path) -> None:
    class JsonlTransport:
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        json.dumps(
                            {
                                "type": "item.completed",
                                "item": {"type": "agent_message", "text": "done"},
                            }
                        ),
                        json.dumps(
                            {
                                "type": "turn.completed",
                                "usage": {"input_tokens": 4, "output_tokens": 2},
                            }
                        ),
                    )
                ),
                "",
                2,
            )

    configured = profile(
        "codex-a",
        provider="codex",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="codex-a",
        state_directory=tmp_path / "codex-a",
    )
    result = await CodexCliAdapter(JsonlTransport()).execute(
        provider_request().model_copy(
            update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
        ),
        configured,
        CancellationContext(),
    )
    assert result.text == "done" and result.usage.input_tokens == 4


@pytest.mark.anyio
async def test_codex_structured_jsonl_promotes_final_typed_object(tmp_path: Path) -> None:
    payload = {
        "schema_version": "step09a-worker-edit-report.v1",
        "experiment_id": "mvp-canary",
        "task_id": "latest-inspect",
        "attempt": 1,
        "claimed_complete": True,
        "implementation_summary": "Implemented the requested change.",
        "limitations": [],
        "failure_classification": None,
        "usage": {
            "input_tokens": None,
            "cached_input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
            "unavailable_reason": "provider report is non-authoritative",
        },
    }
    transport = StaticCodexTransport(codex_jsonl(json.dumps(payload)), duration_ms=73)
    result = await CodexCliAdapter(transport).execute(
        codex_request(schema=WorkerEditReport.model_json_schema()),
        codex_profile(tmp_path),
        CancellationContext(),
    )

    assert result.text is None
    assert result.structured_output == payload
    assert result.provider_session_id == "session-safe"
    assert result.usage.input_tokens == 143
    assert result.usage.cached_tokens == 21
    assert result.usage.output_tokens == 17
    assert result.usage.duration_ms == 73


@pytest.mark.anyio
async def test_codex_unstructured_json_looking_text_is_not_promoted(tmp_path: Path) -> None:
    text = '{"looks":"structured"}'
    result = await CodexCliAdapter(StaticCodexTransport(codex_jsonl(text))).execute(
        codex_request(), codex_profile(tmp_path), CancellationContext()
    )
    assert result.text == text
    assert result.structured_output is None


@pytest.mark.anyio
async def test_codex_rejects_invalid_requested_schema_before_transport(tmp_path: Path) -> None:
    transport = StaticCodexTransport(codex_jsonl("{}"))
    with pytest.raises(ProviderFailure) as captured:
        await CodexCliAdapter(transport).execute(
            codex_request(schema={"type": "not-a-json-schema-type"}),
            codex_profile(tmp_path),
            CancellationContext(),
        )
    assert captured.value.category is ProviderErrorCategory.PERMANENT_REQUEST
    assert captured.value.retryable is False
    assert captured.value.request_sent is False
    assert captured.value.__cause__ is None
    assert transport.invocations == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "returned",
    (
        "not-json SECRET_RETURNED_VALUE",
        '```json\n{"value":"ok"}\n```',
        'result: {"value":"ok"}',
        '[{"value":"ok"}]',
        '"scalar"',
        '{"unexpected":"field"}',
    ),
)
async def test_codex_structured_output_failures_are_safe(tmp_path: Path, returned: str) -> None:
    schema = {
        "type": "object",
        "properties": {"value": {"type": "string"}},
        "required": ["value"],
        "additionalProperties": False,
    }
    with pytest.raises(ProviderFailure) as captured:
        await CodexCliAdapter(StaticCodexTransport(codex_jsonl(returned))).execute(
            codex_request(schema=schema), codex_profile(tmp_path), CancellationContext()
        )
    failure = captured.value
    assert failure.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
    assert failure.retryable is True
    assert failure.request_sent is True
    assert failure.__cause__ is None
    assert returned not in str(failure)
    assert "SECRET_RETURNED_VALUE" not in str(failure)


@pytest.mark.anyio
async def test_codex_full_object_structured_output_is_validated_and_preferred(
    tmp_path: Path,
) -> None:
    body = {
        "text": '{"value":"text"}',
        "structured_output": {"value": "direct"},
        "request_id": "request-safe",
        "session_id": "session-safe",
        "finish_reason": "stop",
        "usage": {"input_tokens": 9, "cached_tokens": 4, "output_tokens": 3},
    }
    schema = {
        "type": "object",
        "properties": {"value": {"const": "direct"}},
        "required": ["value"],
        "additionalProperties": False,
    }
    result = await CodexCliAdapter(StaticCodexTransport(json.dumps(body), 41)).execute(
        codex_request(schema=schema), codex_profile(tmp_path), CancellationContext()
    )
    assert result.structured_output == {"value": "direct"}
    assert result.text is None
    assert result.provider_request_id == "request-safe"
    assert result.provider_session_id == "session-safe"
    assert result.finish_reason == "stop"
    assert result.usage == NormalizedUsage(
        input_tokens=9, cached_tokens=4, output_tokens=3, duration_ms=41
    )


@pytest.mark.anyio
async def test_grok_adapter_uses_strict_headless_contract(tmp_path: Path) -> None:
    class GrokTransport(FakeCodexTransport):
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            self.invocations.append(invocation)
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        json.dumps([]),
                        json.dumps({"type": "thought", "data": "private reasoning"}),
                        json.dumps({"type": "text", "data": "safe "}),
                        json.dumps({"type": "text", "data": "result"}),
                        json.dumps(
                            {
                                "type": "end",
                                "stopReason": "EndTurn",
                                "sessionId": "session",
                                "requestId": "request",
                                "usage": {
                                    "input_tokens": 7,
                                    "output_tokens": 2,
                                    "cache_read_input_tokens": 3,
                                },
                            }
                        ),
                    )
                ),
                "",
                12,
            )

    transport = GrokTransport()
    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    result = await GrokCliAdapter(transport).execute(
        provider_request().model_copy(
            update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
        ),
        configured,
        CancellationContext(),
    )
    invocation = transport.invocations[0]
    assert result.text == "safe result"
    assert result.provider_request_id == "request"
    assert result.usage.cached_tokens == 3
    assert invocation.argv[:2] == ("grok", "--no-auto-update")
    assert invocation.argv[2:4] == ("--prompt-file", "/dev/stdin")
    assert "dontAsk" in invocation.argv and "strict" in invocation.argv
    assert "Read(/grok-home/**)" in invocation.argv
    assert "Edit(/grok-home/**)" in invocation.argv
    assert "Bash(git *)" in invocation.argv
    assert "Bash(tail -n 1 README.md)" in invocation.argv
    assert "Bash(date +%s%3N)" in invocation.argv
    assert "Bash(*)" not in invocation.argv
    assert "auth.json" not in invocation.stdin
    assert "do not use cd" in invocation.stdin
    assert (await GrokCliAdapter(transport).health(configured)).healthy
    from vuzol.providers.grok import _usage_int

    assert _usage_int([], "input_tokens") is None


@pytest.mark.anyio
async def test_grok_adapter_fails_closed_for_invalid_execution(tmp_path: Path) -> None:
    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    request = provider_request()
    adapter = GrokCliAdapter(FakeCodexTransport())
    with pytest.raises(ProviderFailure, match="isolation"):
        await adapter.execute(
            request.model_copy(
                update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
            ),
            configured.model_copy(update={"runtime_identity": None}),
            CancellationContext(),
        )
    with pytest.raises(ProviderFailure, match="requires an isolated"):
        await adapter.execute(request, configured, CancellationContext())

    class FailedTransport:
        def __init__(self, result: CodexProcessResult | None = None) -> None:
            self.result = result

        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            if self.result is None:
                raise RuntimeError("transport unavailable")
            return self.result

    fenced = request.model_copy(
        update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
    )
    for transport in (
        FailedTransport(),
        FailedTransport(CodexProcessResult(1, "", "failed", 1)),
        FailedTransport(CodexProcessResult(0, '{"type":"text","data":"partial"}', "", 1)),
    ):
        with pytest.raises(ProviderFailure):
            await GrokCliAdapter(transport).execute(fenced, configured, CancellationContext())

    class InvalidInvocationTransport(FailedTransport):
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            raise ValueError("invalid invocation")

    with pytest.raises(ValueError, match="invalid invocation"):
        await GrokCliAdapter(InvalidInvocationTransport()).execute(
            fenced, configured, CancellationContext()
        )


@pytest.mark.anyio
async def test_grok_adapter_classifies_structured_provider_cancellation(tmp_path: Path) -> None:
    class CancelledTransport:
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        '{"type":"thought","data":"sensitive model output"}',
                        '{"type":"end","stopReason":"Cancelled","requestId":"request"}',
                    )
                ),
                "",
                75_700,
            )

    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    fenced = provider_request().model_copy(
        update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
    )
    with pytest.raises(ProviderFailure) as captured:
        await GrokCliAdapter(CancelledTransport()).execute(
            fenced, configured, CancellationContext()
        )
    assert captured.value.category is ProviderErrorCategory.CANCELLED
    assert captured.value.retryable is False


@pytest.mark.anyio
async def test_grok_adapter_validates_step09a_edit_report(tmp_path: Path) -> None:
    manifest = {
        "schema_version": "step09a-worker-edit-report.v1",
        "experiment_id": "experiment",
        "task_id": "task",
        "claimed_complete": True,
        "implementation_summary": "Implemented the requested change.",
        "usage": {
            "input_tokens": None,
            "cached_input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
            "unavailable_reason": "The worker does not receive provider accounting.",
        },
        "failure_classification": None,
        "limitations": [],
        "attempt": 1,
    }

    invocations: list[CodexInvocation] = []

    class ManifestTransport:
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del cancellation
            invocations.append(invocation)
            return CodexProcessResult(
                0,
                "\n".join(
                    (
                        json.dumps({"type": "text", "data": json.dumps(manifest)}),
                        json.dumps({"type": "end", "stopReason": "EndTurn"}),
                    )
                ),
                "",
                10,
            )

    configured = profile(
        "grok-a",
        provider="grok",
        model="grok-build",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    request = provider_request().model_copy(
        update={
            "sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001",
            "output_schema_version": "step09a-worker-edit-report.v1",
        }
    )
    result = await GrokCliAdapter(ManifestTransport()).execute(
        request, configured, CancellationContext()
    )
    assert result.structured_output == manifest
    prompt = json.loads(invocations[-1].stdin)
    instruction = prompt["execution_policy"]["result_manifest"]
    assert "Do not invoke shell commands, Git, or project gates" in instruction
    assert "Vuzol owns inspection, gates, staging, commit creation" in instruction
    shell_instruction = prompt["execution_policy"]["shell_invocation"]
    assert "Do not invoke native shell tools" in shell_instruction
    assert "git, make, or ./verify.sh" not in shell_instruction

    usage = manifest["usage"]
    assert isinstance(usage, dict)
    invalid = {**manifest, "usage": {**usage, "unavailable_reason": None}}
    manifest.clear()
    manifest.update(invalid)
    with pytest.raises(ProviderFailure) as captured:
        await GrokCliAdapter(ManifestTransport()).execute(
            request, configured, CancellationContext()
        )
    assert captured.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
    assert captured.value.retryable is False


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


def test_grok_event_summary_is_content_free_for_completed_response() -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        "\n".join(
            (
                '{"type":"thought","data":"private prompt and output"}',
                '{"type":"end","stopReason":"Cancelled","requestId":"secret-id"}',
            )
        )
    )
    serialized = json.dumps(summary)
    assert summary["event_count"] == 2
    assert summary["last_event_type"] == "end"
    assert summary["last_stop_reason"] == "Cancelled"
    assert summary["schema_version"] == "grok-event-summary.v2"
    assert summary["cancellation_evidence_category"] == "PROVIDER_CANCELLED_UNATTRIBUTED"
    assert summary["evidence_completeness"] == "unavailable"
    assert "private prompt" not in serialized
    assert "secret-id" not in serialized

    completed = summarize_grok_events(
        "\n".join(
            (
                '{"type":"text","data":"private final response"}',
                '{"type":"end","stopReason":"EndTurn"}',
            )
        )
    )
    assert completed["cancellation_evidence_category"] is None
    assert completed["final_text_generation_began"] is True
    assert "private final response" not in json.dumps(completed)


@pytest.mark.parametrize(
    "session_id",
    ("../escape", "nested/session", "session\\name", "", "space value"),
)
def test_grok_session_id_rejects_traversal_and_malformed_values(session_id: str) -> None:
    from vuzol.providers.grok import grok_session_id_from_stdout, staged_grok_diagnostic_paths

    stdout = json.dumps({"type": "end", "stopReason": "EndTurn", "sessionId": session_id})
    assert grok_session_id_from_stdout(stdout) is None
    assert staged_grok_diagnostic_paths(Path("/safe/staging"), session_id) is None


def test_grok_session_id_selects_only_the_final_exact_protocol_session() -> None:
    from vuzol.providers.grok import grok_session_id_from_stdout

    older = "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    exact = "019f6149-44c0-7520-932c-5e0f41c99351"
    stdout = "\n".join(
        (
            f'{{"type":"end","sessionId":"{older}"}}',
            '{"type":"thought","sessionId":"019f0000-0000-7000-8000-000000000000"}',
            f'{{"type":"end","sessionId":"{exact}"}}',
        )
    )
    assert grok_session_id_from_stdout(stdout) == exact


def test_grok_event_summary_proves_permission_cancellation_and_correlates_sequences() -> None:
    from vuzol.providers.grok import summarize_grok_events

    stdout = "\n".join(
        (
            '{"type":"thought","data":"SECRET_THOUGHT_PAYLOAD"}',
            (
                '{"type":"end","stopReason":"Cancelled",'
                '"sessionId":"019f5e8d-d90b-7e40-a698-8a71fa87eff8",'
                '"requestId":"019f5e8d-d90b-7e40-a698-8a71fa87eff9"}'
            ),
        )
    )
    summary = summarize_grok_events(
        stdout,
        diagnostic_events=_grok_diagnostic_events(
            decision="cancelled", completed=False, category="permission_cancelled"
        ).splitlines(),
        session_updates=_grok_tool_updates("make test", completed=False).splitlines(),
    )
    serialized = json.dumps(summary, sort_keys=True)
    assert summary["cancellation_evidence_category"] == "PROVIDER_PERMISSION_CANCELLED"
    assert summary["last_permission_decision"] == "cancelled"
    assert summary["last_safe_command_identity"] == "make test"
    assert summary["last_tool_kind"] == "Bash"
    assert summary["last_native_tool_request_sequence"] == 2
    assert summary["last_permission_event_sequence"] == 4
    assert summary["last_native_tool_result_sequence"] is None
    assert summary["last_tool_result_received"] is False
    assert summary["cancellation_stage"] == "after_permission_cancellation_before_execution"
    assert summary["evidence_completeness"] == "complete"
    assert summary["provider_session_id"] == "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    for unsafe in (
        "SECRET_THOUGHT_PAYLOAD",
        "private task detail",
        "private tool output",
        "unsafe display title",
    ):
        assert unsafe not in serialized


@pytest.mark.parametrize(
    ("category", "classification"),
    (
        ("invalid_tool", "INVALID_TOOL_INVOCATION"),
        ("cancelled", "PROVIDER_INTERNAL_CANCELLED"),
    ),
)
def test_grok_event_summary_uses_first_party_cancellation_category(
    category: str, classification: str
) -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        '{"type":"end","stopReason":"Cancelled"}',
        diagnostic_events=_grok_diagnostic_events(
            decision="allow", completed=False, category=category
        ).splitlines(),
        session_updates=_grok_tool_updates("make type-check", completed=False).splitlines(),
    )
    assert summary["cancellation_evidence_category"] == classification
    assert summary["last_permission_decision"] == "allowed"
    assert summary["cancellation_stage"] == "during_tool_execution"


@pytest.mark.parametrize(
    ("decision", "completed", "expected_decision", "expected_result"),
    (("allow", True, "allowed", True), ("deny", False, "denied", False)),
)
def test_grok_event_summary_tracks_permission_and_tool_result(
    decision: str, completed: bool, expected_decision: str, expected_result: bool
) -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        '{"type":"end","stopReason":"EndTurn"}',
        diagnostic_events=_grok_diagnostic_events(
            decision=decision, completed=completed
        ).splitlines(),
        session_updates=_grok_tool_updates("make lint", completed=completed).splitlines(),
    )
    assert summary["last_permission_decision"] == expected_decision
    assert summary["last_tool_result_received"] is expected_result
    assert summary["last_native_tool_result_sequence"] == (5 if completed else None)
    if completed:
        assert summary["last_completed_tool_action"] is not None
    else:
        assert summary["last_completed_tool_action"] is None


@pytest.mark.parametrize(
    ("command", "family", "flag"),
    (
        ("custom-tool --password SECRET_COMMAND_VALUE", "unknown", "unknown_command_family"),
        ("cd /private && make test", "cd", "shell_chain_operators"),
        ("/bin/sh -c 'make test'", "shell", "shell_wrapper"),
        ("TOKEN=SECRET make test", "unknown", "environment_prefix"),
        ("uv run pytest", "uv", "direct_tool_invocation"),
    ),
)
def test_grok_event_summary_hashes_unsafe_commands_without_retaining_them(
    command: str, family: str, flag: str
) -> None:
    from vuzol.providers.grok import summarize_grok_events

    summary = summarize_grok_events(
        '{"type":"end","stopReason":"Cancelled"}',
        diagnostic_events=_grok_diagnostic_events(
            decision="cancelled", completed=False, category="permission_cancelled"
        ).splitlines(),
        session_updates=_grok_tool_updates(command, completed=False).splitlines(),
    )
    serialized = json.dumps(summary)
    assert summary["last_safe_command_identity"] is None
    assert summary["last_command_family"] == family
    assert summary["last_command_byte_length"] == len(command.encode())
    assert summary["last_command_sha256"] == hashlib.sha256(command.encode()).hexdigest()
    flags = summary["last_command_structural_flags"]
    assert isinstance(flags, dict)
    assert flags[flag] is True
    assert command not in serialized
    assert "SECRET" not in serialized


def test_grok_event_summary_drops_edit_read_grep_paths_and_payloads() -> None:
    from vuzol.providers.grok import summarize_grok_events

    diagnostic = "\n".join(
        json.dumps(event)
        for event in (
            {"type": "turn_started", "schema_version": "1.0"},
            {"type": "tool_started", "tool_name": "read_file"},
            {"type": "tool_completed", "tool_name": "read_file", "outcome": "success"},
            {"type": "tool_started", "tool_name": "grep"},
            {"type": "tool_completed", "tool_name": "grep", "outcome": "success"},
            {"type": "tool_started", "tool_name": "search_replace"},
            {
                "type": "tool_completed",
                "tool_name": "search_replace",
                "outcome": "success",
            },
        )
    )
    updates = "\n".join(
        json.dumps(
            {
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": f"call-1aa3af3d-e549-4c73-ac4e-fc0c08302ed{i}-3{i}",
                        "rawInput": {
                            "path": f"/SECRET/PATH/{name}",
                            "pattern": "SECRET_GREP_PATTERN",
                            "new_string": "SECRET_EDIT_CONTENT",
                        },
                        "_meta": {"x.ai/tool": {"name": name}},
                    }
                },
            }
        )
        for i, name in enumerate(("read_file", "grep", "search_replace"))
    )
    summary = summarize_grok_events(
        '{"type":"end","stopReason":"EndTurn"}',
        diagnostic_events=diagnostic.splitlines(),
        session_updates=updates.splitlines(),
    )
    serialized = json.dumps(summary)
    assert summary["last_tool_kind"] == "Edit"
    assert summary["diagnostic_tool_count"] == 3
    assert "SECRET" not in serialized
    assert "/PATH/" not in serialized


def test_grok_event_summary_is_malformed_safe_and_bounded() -> None:
    from vuzol.providers.grok import summarize_grok_events

    stdout = "\n".join(["not-json", *('{"type":"thought","data":"hidden"}' for _ in range(140))])
    diagnostic = [
        '{"type":"turn_started","schema_version":"1.0"}',
        "malformed",
        *(
            json.dumps({"type": "phase_changed", "phase": "streaming_reasoning"})
            for _ in range(140)
        ),
    ]
    summary = summarize_grok_events(stdout, diagnostic_events=diagnostic, session_updates=[])
    assert summary["event_count"] == 140
    assert summary["retained_event_count"] == 128
    assert summary["events_truncated"] is True
    assert summary["malformed_event_count"] == 1
    assert summary["diagnostic_event_count"] == 142
    assert summary["retained_diagnostic_event_count"] == 128
    assert summary["diagnostic_events_truncated"] is True
    assert summary["malformed_diagnostic_event_count"] == 1
    assert summary["evidence_completeness"] == "partial"


@pytest.mark.anyio
async def test_codex_adapter_rejects_missing_isolation_sandbox_and_bad_output(
    tmp_path: Path,
) -> None:
    transport = FakeCodexTransport()
    configured = profile(
        "codex-a",
        provider="codex",
        api_base_url=None,
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        credential_required=False,
        runtime_identity="codex-a",
        state_directory=tmp_path / "codex-a",
    )
    adapter = CodexCliAdapter(transport)
    with pytest.raises(ProviderFailure, match="isolation"):
        await adapter.execute(
            provider_request(),
            configured.model_copy(update={"runtime_identity": None}),
            CancellationContext(),
        )
    with pytest.raises(ProviderFailure, match="requires an isolated"):
        await adapter.execute(provider_request(), configured, CancellationContext())

    class BadTransport:
        async def run(
            self, invocation: CodexInvocation, cancellation: CancellationContext
        ) -> CodexProcessResult:
            del invocation, cancellation
            return CodexProcessResult(0, "not-json", "", 1)

    with pytest.raises(ProviderFailure) as captured:
        await CodexCliAdapter(BadTransport()).execute(
            provider_request().model_copy(
                update={"sandbox_reference": "worktree:00000000-0000-0000-0000-000000000001"}
            ),
            configured,
            CancellationContext(),
        )
    assert captured.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT


def test_adapter_registry_resolves_only_selected_api_profile(tmp_path: Path) -> None:
    configured = profile(
        "api",
        credential_reference="env:API_KEY",
    )
    resolver = ScopedSecretResolver(
        access_policy={"env:API_KEY": frozenset({"profile:api"})},
        secret_file_root=tmp_path,
        environment={"API_KEY": "scoped-value"},  # pragma: allowlist secret
    )
    registry = AdapterRegistry(ProfileRegistry((configured,)), resolver)
    assert isinstance(registry.get("api"), OpenAICompatibleAdapter)
    assert registry.get("api") is registry.get("api")

    cli = configured.model_copy(
        update={
            "id": "cli",
            "provider": "codex",
            "api_base_url": None,
            "launch_mode": LaunchMode.CLI,
            "credential_reference": None,
            "credential_required": False,
            "runtime_identity": "codex",
            "state_directory": tmp_path / "codex",
        }
    )
    with pytest.raises(ValueError, match="Step 08"):
        AdapterRegistry(ProfileRegistry((cli,)), resolver).get("cli")
