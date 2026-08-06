from __future__ import annotations

import json
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema.exceptions import SchemaError

from vuzol.config.models import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.providers.domain import ProviderErrorCategory, ProviderRequest
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.kimi import KIMI_MODEL, KimiCliAdapter, canonical_kimi_argv
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.workflows.ports import CancellationContext


class Transport:
    def __init__(self) -> None:
        self.invocation: CodexInvocation | None = None

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        del cancellation
        self.invocation = invocation
        return CodexProcessResult(
            exit_code=0,
            stdout='{"role":"assistant","content":"done"}\n',
            stderr="",
            duration_ms=12,
        )


class ResultTransport(Transport):
    def __init__(self, result: CodexProcessResult | Exception) -> None:
        super().__init__()
        self.result = result

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        del cancellation
        self.invocation = invocation
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def profile() -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id="tokenrouter-kimi-a",
        provider="kimi",
        model=KIMI_MODEL,
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.CHEAP,
        roles=frozenset({ProviderRole.EXECUTOR}),
        supported_task_types=frozenset({"coding"}),
        runtime_identity="vuzol-kimi-a",
        state_directory=Path("/var/lib/vuzol-provider-state/kimi-test"),
    )


def request() -> ProviderRequest:
    return ProviderRequest(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        role=ProviderRole.EXECUTOR,
        original_input="change it",
        system_policy_revision="test-policy",
        prompt_revision="test-prompt",
        required_capabilities=frozenset({Capability.CODE_EDIT}),
        max_input_tokens=10_000,
        max_output_tokens=1_000,
        reserved_cost_units=Decimal(1),
        reserved_quota_units=Decimal(1),
        timeout_seconds=60,
        sandbox_reference="worktree:" + str(uuid.uuid4()),
        provider_attempt=1,
        lease_generation=1,
    )


def test_canonical_kimi_command_is_model_allowlisted() -> None:
    execute = canonical_kimi_argv(KIMI_MODEL)
    plan = canonical_kimi_argv(KIMI_MODEL, read_only=True)
    assert execute[0:2] == ("sh", "-c")
    assert "--auto" in execute[2]
    assert "--plan" not in execute[2]
    assert "--plan" in plan[2]
    with pytest.raises(ValueError, match="allowlisted"):
        canonical_kimi_argv("another-model")


@pytest.mark.anyio
async def test_adapter_keeps_task_prompt_on_stdin() -> None:
    transport = Transport()
    result = await KimiCliAdapter(transport).execute(request(), profile(), CancellationContext())

    assert result.text == "done"
    assert transport.invocation is not None
    assert "change it" not in " ".join(transport.invocation.argv)
    assert "change it" in transport.invocation.stdin


@pytest.mark.anyio
async def test_adapter_requires_profile_and_sandbox_isolation() -> None:
    adapter = KimiCliAdapter(Transport())
    incomplete = profile().model_copy(update={"runtime_identity": None})
    with pytest.raises(ProviderFailure) as missing_profile:
        await adapter.execute(request(), incomplete, CancellationContext())
    assert missing_profile.value.category is ProviderErrorCategory.PERMANENT_REQUEST
    assert missing_profile.value.request_sent is False

    unsandboxed = request().model_copy(update={"sandbox_reference": None})
    with pytest.raises(ProviderFailure) as missing_sandbox:
        await adapter.execute(unsandboxed, profile(), CancellationContext())
    assert missing_sandbox.value.category is ProviderErrorCategory.UNSUPPORTED_CAPABILITY


@pytest.mark.anyio
async def test_adapter_builds_read_only_structured_request() -> None:
    transport = ResultTransport(
        CodexProcessResult(
            exit_code=0,
            stdout='{"role":"assistant","content":"{\\"answer\\": 7}"}\n',
            stderr="",
            duration_ms=8,
        )
    )
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "integer"}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    read_only = request().model_copy(
        update={
            "required_capabilities": frozenset({Capability.REPOSITORY_READ}),
            "output_json_schema": schema,
        }
    )
    result = await KimiCliAdapter(transport).execute(read_only, profile(), CancellationContext())
    assert result.text is None
    assert result.structured_output == {"answer": 7}
    assert result.usage.duration_ms == 8
    assert transport.invocation is not None
    assert transport.invocation.argv[-1] == "vuzol-kimi-plan"
    assert "--plan" in transport.invocation.argv[2]
    assert "Inspect only" in transport.invocation.stdin


@pytest.mark.anyio
async def test_adapter_rejects_invalid_output_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "vuzol.providers.kimi.Draft202012Validator.check_schema",
        lambda _schema: (_ for _ in ()).throw(SchemaError("bad")),
    )
    invalid = request().model_copy(update={"output_json_schema": {"type": "bad"}})
    with pytest.raises(ProviderFailure) as caught:
        await KimiCliAdapter(Transport()).execute(invalid, profile(), CancellationContext())
    assert caught.value.category is ProviderErrorCategory.PERMANENT_REQUEST
    assert caught.value.request_sent is False


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("transport_result", "category", "summary"),
    [
        (RuntimeError("transport"), ProviderErrorCategory.PROVIDER_UNAVAILABLE, "transport"),
        (
            RuntimeError("sandbox execution timed out after start"),
            ProviderErrorCategory.TIMEOUT,
            "timed out",
        ),
        (
            CodexProcessResult(exit_code=2, stdout="", stderr="failed", duration_ms=1),
            ProviderErrorCategory.PROVIDER_UNAVAILABLE,
            "invocation failed",
        ),
        (
            CodexProcessResult(exit_code=0, stdout="not-json\n", stderr="", duration_ms=1),
            ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
            "invalid structured output",
        ),
        (
            CodexProcessResult(
                exit_code=0,
                stdout='{"role":"tool","content":"ignored"}\n',
                stderr="",
                duration_ms=1,
            ),
            ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
            "invalid structured output",
        ),
    ],
)
async def test_adapter_normalizes_transport_and_output_failures(
    transport_result: CodexProcessResult | Exception,
    category: ProviderErrorCategory,
    summary: str,
) -> None:
    with pytest.raises(ProviderFailure) as caught:
        await KimiCliAdapter(ResultTransport(transport_result)).execute(
            request(), profile(), CancellationContext()
        )
    assert caught.value.category is category
    assert caught.value.retryable is True
    assert caught.value.request_sent is True
    assert summary in caught.value.safe_summary


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("stderr", "category"),
    [
        ("provider.connection_error: Connection error", ProviderErrorCategory.PROVIDER_UNAVAILABLE),
        ("401 unauthorized", ProviderErrorCategory.AUTHENTICATION),
        ("429 rate limit", ProviderErrorCategory.RATE_LIMITED),
    ],
)
async def test_adapter_classifies_tokenrouter_cli_failures(
    stderr: str, category: ProviderErrorCategory
) -> None:
    failed = CodexProcessResult(exit_code=1, stdout="", stderr=stderr, duration_ms=1)
    with pytest.raises(ProviderFailure) as caught:
        await KimiCliAdapter(ResultTransport(failed)).execute(
            request(), profile(), CancellationContext()
        )
    assert caught.value.category is category


@pytest.mark.anyio
async def test_adapter_rejects_structured_non_object_and_schema_mismatch() -> None:
    schema = {"type": "object", "required": ["answer"]}
    structured_request = request().model_copy(update={"output_json_schema": schema})
    for content in ("[]", "{}"):
        transport = ResultTransport(
            CodexProcessResult(
                exit_code=0,
                stdout=json.dumps({"role": "assistant", "content": content}) + "\n",
                stderr="",
                duration_ms=1,
            )
        )
        with pytest.raises(ProviderFailure) as caught:
            await KimiCliAdapter(transport).execute(
                structured_request, profile(), CancellationContext()
            )
        assert caught.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT


@pytest.mark.anyio
async def test_adapter_propagates_allowlist_value_error_and_health() -> None:
    bad_profile = profile().model_copy(update={"model": "not-allowlisted"})
    with pytest.raises(ValueError, match="allowlisted"):
        await KimiCliAdapter(Transport()).execute(request(), bad_profile, CancellationContext())
    assert await KimiCliAdapter(Transport()).health(profile()) == await KimiCliAdapter(
        Transport()
    ).health(profile())


@pytest.mark.anyio
async def test_adapter_propagates_transport_value_error_and_rejects_empty_response() -> None:
    with pytest.raises(ValueError, match="bad invocation"):
        await KimiCliAdapter(ResultTransport(ValueError("bad invocation"))).execute(
            request(), profile(), CancellationContext()
        )

    empty = CodexProcessResult(
        exit_code=0,
        stdout='{"role":"assistant","content":""}\n',
        stderr="",
        duration_ms=1,
    )
    with pytest.raises(ProviderFailure, match="no final response"):
        await KimiCliAdapter(ResultTransport(empty)).execute(
            request(), profile(), CancellationContext()
        )

    chunks = CodexProcessResult(
        exit_code=0,
        stdout=(
            '{"role":"assistant","content":"part one "}\n'
            '{"role":"assistant","content":"part two"}\n'
        ),
        stderr="",
        duration_ms=1,
    )
    result = await KimiCliAdapter(ResultTransport(chunks)).execute(
        request(), profile(), CancellationContext()
    )
    assert result.text == "part one part two"
