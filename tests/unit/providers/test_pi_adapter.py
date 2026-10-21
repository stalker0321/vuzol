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
    snapshot = canonical_pi_argv("kimi-k3", read_only=True, prompt_only=True)
    assert "--no-tools" in snapshot and "--tools" not in snapshot
    assert "--no-extensions" in snapshot and "--no-context-files" in snapshot
    with pytest.raises(ValueError, match="read-only"):
        canonical_pi_argv("kimi-k3", prompt_only=True)
    with pytest.raises(ValueError, match="model"):
        canonical_pi_argv("  ")


@pytest.mark.anyio
@pytest.mark.parametrize("effort", ["off", "minimal", "low", "medium", "high", "xhigh", "max"])
@pytest.mark.parametrize("snapshot", [False, True])
async def test_reasoning_choice_reaches_the_pinned_transport(effort, snapshot) -> None:
    from vuzol.execution.codex import _require_provider_command

    transport = Transport((FIXTURES / "print-json-text.jsonl").read_text())
    chosen = profile().model_copy(update={"model_reasoning_effort": effort})
    changes = (
        {
            "role": ProviderRole.LEAD,
            "sandbox_reference": "lead-turn:test",
            "required_capabilities": frozenset(),
        }
        if snapshot
        else {}
    )
    await PiCliAdapter(transport).execute(request(**changes), chosen, CancellationContext())
    argv = transport.invocation.argv
    assert argv[argv.index("--thinking") + 1] == effort
    _require_provider_command(
        argv,
        "pi",
        chosen.model,
        reasoning_effort=effort,
        require_read_only=snapshot,
        require_prompt_only=snapshot,
    )
    with pytest.raises(ValueError, match="canonical"):
        _require_provider_command(
            argv,
            "pi",
            chosen.model,
            reasoning_effort=None,
            require_read_only=snapshot,
            require_prompt_only=snapshot,
        )


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
@pytest.mark.parametrize("role", [ProviderRole.LEAD, ProviderRole.REVIEWER])
async def test_turn_inference_disables_tools_but_worktree_review_keeps_inspection(role) -> None:
    for sandbox, prompt_only in (
        ("lead-turn:" + str(uuid.uuid4()), True),
        ("worktree:" + str(uuid.uuid4()), False),
    ):
        transport = Transport((FIXTURES / "print-json-text.jsonl").read_text())
        await PiCliAdapter(transport).execute(
            request(
                role=role,
                sandbox_reference=sandbox,
                required_capabilities=frozenset({Capability.REPOSITORY_READ}),
            ),
            profile(),
            CancellationContext(),
        )
        argv = transport.invocation.argv
        assert ("--no-tools" in argv) is prompt_only
        assert ("--tools" in argv) is not prompt_only
        assert ("--system-prompt" in argv) is prompt_only


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


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("content", "stage", "path", "fenced"),
    [
        ('```json\n{"answer":"ok"}\n```', "json_parse", None, True),
        ('{"answer":', "json_parse", None, False),
        ('["secret-response-value"]', "non_object", None, False),
        (
            '{"answer":"secret-response-value"}',
            "schema_validation",
            "properties/answer/maxLength",
            False,
        ),
        ('{"foreign-secret-key":"secret-response-value"}', "schema_validation", "required", False),
    ],
)
async def test_failure_retains_structural_diagnostics_not_response_secrets(
    content, stage, path, fenced
):
    import hashlib

    from vuzol.providers.domain import ProviderResult

    message = {
        "role": "assistant",
        "stopReason": "stop",
        "content": [{"type": "text", "text": content}],
        "usage": {"input": 4435, "output": 2460},
        "responseId": "measured-failure",
    }
    stdout = json.dumps({"type": "agent_end", "messages": [message]}) + '\n{"type":"agent_settled"}'
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string", "maxLength": 2}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    with pytest.raises(ProviderFailure) as caught:
        await PiCliAdapter(Transport(stdout)).execute(
            request(output_json_schema=schema), profile(), CancellationContext()
        )
    failure = caught.value
    assert failure.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
    receipt = failure.receipt_result()
    assert receipt is not None and receipt.failure is not None
    restored = ProviderResult.model_validate_json(receipt.model_dump_json())
    assert restored.failure == failure.details()
    assert restored.usage.input_tokens == 4435 and restored.usage.output_tokens == 2460
    assert restored.provider_request_id == "measured-failure"
    diagnostic = restored.failure.diagnostic
    assert diagnostic.stage == stage and diagnostic.schema_path == path
    assert diagnostic.content_sha256 == hashlib.sha256(content.encode()).hexdigest()
    assert diagnostic.content_bytes == len(content.encode())
    assert diagnostic.json_fenced is fenced
    assert "secret-response-value" not in restored.model_dump_json()
    assert "foreign-secret-key" not in restored.model_dump_json()


def test_old_receipts_remain_readable_without_diagnostics():
    from vuzol.providers.domain import ProviderResult

    old = {"status": "failed", "usage": {"duration_ms": 1}, "adapter_version": "failure-receipt.v1"}
    assert ProviderResult.model_validate(old).failure is None
