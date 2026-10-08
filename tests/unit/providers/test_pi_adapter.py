from __future__ import annotations

import json
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from vuzol.config.models import (
    Capability,
    CostClass,
    LaunchMode,
    ProviderProfileConfig,
    ProviderRole,
)
from vuzol.providers.domain import ProviderErrorCategory, ProviderRequest
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.pi import PI_PROVIDER, PiCliAdapter, canonical_pi_argv
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.workflows.ports import CancellationContext

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "providers" / "pi"


class Transport:
    def __init__(self, stdout: str, *, exit_code: int = 0) -> None:
        self.stdout = stdout
        self.exit_code = exit_code
        self.invocation: CodexInvocation | None = None

    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult:
        del cancellation
        self.invocation = invocation
        return CodexProcessResult(
            exit_code=self.exit_code, stdout=self.stdout, stderr="", duration_ms=12
        )


def profile() -> ProviderProfileConfig:
    return ProviderProfileConfig(
        id="pi-opencode-go-a",
        provider="pi",
        model="kimi-k3",
        launch_mode=LaunchMode.CLI,
        credential_required=False,
        capabilities=frozenset({Capability.REPOSITORY_READ, Capability.CODE_EDIT}),
        concurrency_limit=1,
        cost_class=CostClass.BALANCED,
        roles=frozenset({ProviderRole.EXECUTOR}),
        supported_task_types=frozenset({"coding"}),
        runtime_identity="vuzol-pi-a",
        state_directory=Path("/var/lib/vuzol-provider-state/pi-test"),
    )


def request(**changes: object) -> ProviderRequest:
    values: dict[str, object] = {
        "task_id": uuid.uuid4(),
        "run_id": uuid.uuid4(),
        "step_id": uuid.uuid4(),
        "role": ProviderRole.EXECUTOR,
        "original_input": "change it",
        "system_policy_revision": "test-policy",
        "prompt_revision": "test-prompt",
        "required_capabilities": frozenset({Capability.CODE_EDIT}),
        "max_input_tokens": 10_000,
        "max_output_tokens": 1_000,
        "reserved_cost_units": Decimal(1),
        "reserved_quota_units": Decimal(1),
        "timeout_seconds": 60,
        "sandbox_reference": "worktree:" + str(uuid.uuid4()),
        "provider_attempt": 1,
        "lease_generation": 1,
    }
    values.update(changes)
    return ProviderRequest.model_validate(values)


def test_canonical_pi_argv_forms() -> None:
    write = canonical_pi_argv("kimi-k3")
    read_only = canonical_pi_argv("kimi-k3", read_only=True)
    assert write[:2] == ("pi", "-p")
    assert "--mode" in write and "json" in write
    assert "--no-session" in write
    assert write[write.index("--provider") + 1] == PI_PROVIDER
    assert write[write.index("--model") + 1] == "kimi-k3"
    assert "--tools" not in write
    assert read_only[: len(write)] == write
    assert read_only[read_only.index("--tools") + 1] == "read,grep,find,ls"
    with pytest.raises(ValueError, match="model"):
        canonical_pi_argv("  ")


@pytest.mark.anyio
async def test_adapter_parses_final_agent_end_and_keeps_prompt_on_stdin() -> None:
    transport = Transport((FIXTURES / "print-json-text.jsonl").read_text())
    result = await PiCliAdapter(transport).execute(request(), profile(), CancellationContext())

    assert result.text == "surface probe reply"
    assert result.finish_reason == "stop"
    assert result.provider_session_id
    assert result.usage.input_tokens == 3400
    assert result.usage.output_tokens == 310
    assert result.usage.cost_units == Decimal("0.00402")
    assert transport.invocation is not None
    assert "change it" not in " ".join(transport.invocation.argv)
    assert "change it" in transport.invocation.stdin


@pytest.mark.anyio
async def test_adapter_settles_on_agent_settled_not_first_agent_end() -> None:
    transport = Transport((FIXTURES / "print-json-retry.jsonl").read_text())
    result = await PiCliAdapter(transport).execute(request(), profile(), CancellationContext())

    # Four agent_end events (three retryable errors + one success); only the
    # final one carries the answer and the real usage.
    assert result.text == "recovered after retries"
    assert result.finish_reason == "stop"
    assert result.usage.input_tokens == 500
    assert result.usage.output_tokens == 20


@pytest.mark.anyio
async def test_adapter_uses_read_only_tools_without_code_edit() -> None:
    transport = Transport((FIXTURES / "print-json-text.jsonl").read_text())
    await PiCliAdapter(transport).execute(
        request(required_capabilities=frozenset({Capability.REPOSITORY_READ})),
        profile(),
        CancellationContext(),
    )
    assert transport.invocation is not None
    assert transport.invocation.argv[transport.invocation.argv.index("--tools") + 1] == (
        "read,grep,find,ls"
    )


@pytest.mark.anyio
async def test_adapter_rejects_stream_without_agent_settled() -> None:
    truncated = "\n".join(
        line
        for line in (FIXTURES / "print-json-text.jsonl").read_text().splitlines()
        if '"agent_settled"' not in line
    )
    with pytest.raises(ProviderFailure) as error:
        await PiCliAdapter(Transport(truncated)).execute(
            request(), profile(), CancellationContext()
        )
    assert error.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT


@pytest.mark.anyio
async def test_adapter_maps_error_stop_reason() -> None:
    stdout = (
        '{"type":"session","id":"s1"}\n'
        '{"type":"agent_end","willRetry":false,"messages":[{"role":"assistant",'
        '"stopReason":"error","content":[{"type":"text","text":""}],'
        '"usage":{"input":0,"output":0,"cost":{"total":0}}}]}\n'
        '{"type":"agent_settled"}\n'
    )
    with pytest.raises(ProviderFailure) as error:
        await PiCliAdapter(Transport(stdout)).execute(request(), profile(), CancellationContext())
    assert error.value.category is ProviderErrorCategory.PROVIDER_UNAVAILABLE


@pytest.mark.anyio
async def test_adapter_validates_structured_output() -> None:
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    good = (
        '{"type":"session","id":"s1"}\n'
        '{"type":"agent_end","willRetry":false,"messages":[{"role":"assistant",'
        '"stopReason":"stop","content":[{"type":"text","text":"{\\"answer\\": \\"ok\\"}"}],'
        '"usage":{"input":1,"output":1,"cost":{"total":0.1}}}]}\n'
        '{"type":"agent_settled"}\n'
    )
    result = await PiCliAdapter(Transport(good)).execute(
        request(output_json_schema=schema), profile(), CancellationContext()
    )
    assert result.text is None
    assert result.structured_output == {"answer": "ok"}

    bad = good.replace('{\\"answer\\": \\"ok\\"}', "not json")
    with pytest.raises(ProviderFailure) as error:
        await PiCliAdapter(Transport(bad)).execute(
            request(output_json_schema=schema), profile(), CancellationContext()
        )
    assert error.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT


@pytest.mark.anyio
async def test_adapter_requires_sandbox_reference() -> None:
    with pytest.raises(ProviderFailure) as error:
        await PiCliAdapter(Transport("")).execute(
            request(sandbox_reference=None), profile(), CancellationContext()
        )
    assert error.value.category is ProviderErrorCategory.UNSUPPORTED_CAPABILITY


def test_decoded_session_id_present_in_fixture() -> None:
    first = (FIXTURES / "print-json-text.jsonl").read_text().splitlines()[0]
    assert json.loads(first)["type"] == "session"
    assert json.loads(first)["id"]
