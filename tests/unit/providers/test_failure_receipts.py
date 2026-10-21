"""Output rejection must not discard independent provider usage receipts."""

import json
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from tests.unit.providers._test_providers_helpers import (
    StaticCodexTransport,
    codex_jsonl,
    codex_profile,
    codex_request,
    profile,
    provider_request,
)
from tests.unit.providers.test_pi_adapter import FIXTURES, Transport
from tests.unit.providers.test_pi_adapter import profile as pi_profile
from tests.unit.providers.test_pi_adapter import request as pi_request
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.domain import NormalizedUsage, ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.openai import OpenAICompatibleAdapter
from vuzol.providers.pi import PiCliAdapter
from vuzol.workflows.ports import CancellationContext

SCHEMA = {"type": "object", "required": ["answer"]}


@pytest.mark.anyio
@pytest.mark.parametrize("missing_answer", [False, True])
async def test_codex_rejection_preserves_tokens(tmp_path: Path, missing_answer: bool) -> None:
    stdout = codex_jsonl("not JSON")
    if missing_answer:
        stdout = "\n".join(line for line in stdout.splitlines() if '"agent_message"' not in line)
    with pytest.raises(ProviderFailure) as caught:
        await CodexCliAdapter(StaticCodexTransport(stdout)).execute(
            codex_request(schema=SCHEMA), codex_profile(tmp_path), CancellationContext()
        )
    assert caught.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == 143
    assert caught.value.usage.output_tokens == 17
    assert caught.value.usage.cached_tokens == 21


@pytest.mark.anyio
@pytest.mark.parametrize("choices", [[], [{"message": {"content": "not JSON"}}]])
async def test_api_rejection_preserves_receipt(choices: list) -> None:
    async with httpx.AsyncClient(
        base_url="https://provider.example/v1",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={
                    "choices": choices,
                    "usage": {"prompt_tokens": 42, "completion_tokens": 7},
                },
                headers={"x-request-id": "receipt-1"},
            )
        ),
    ) as client:
        with pytest.raises(ProviderFailure) as caught:
            await OpenAICompatibleAdapter(credential=SecretStr("test-key"), client=client).execute(
                provider_request(structured=True), profile("api"), CancellationContext()
            )
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == 42
    assert caught.value.usage.output_tokens == 7
    assert caught.value.provider_request_id == "receipt-1"


@pytest.mark.anyio
@pytest.mark.parametrize("settled", [False, True])
async def test_pi_rejection_preserves_receipt(settled: bool) -> None:
    stdout = (FIXTURES / "print-json-text.jsonl").read_text()
    if not settled:
        stdout = "\n".join(line for line in stdout.splitlines() if '"agent_settled"' not in line)
    with pytest.raises(ProviderFailure) as caught:
        await PiCliAdapter(Transport(stdout)).execute(
            pi_request(output_json_schema=SCHEMA), pi_profile(), CancellationContext()
        )
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == 3400
    assert caught.value.usage.output_tokens == 310


@pytest.mark.anyio
@pytest.mark.parametrize("content", [None, 17, []])
async def test_text_contract_does_not_stringify_absent_or_nontext_answer(content: object) -> None:
    async with httpx.AsyncClient(
        base_url="https://provider.example/v1",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": content}}],
                    "usage": {"prompt_tokens": 42, "completion_tokens": 7},
                },
            )
        ),
    ) as client:
        with pytest.raises(ProviderFailure) as caught:
            await OpenAICompatibleAdapter(credential=SecretStr("test-key"), client=client).execute(
                provider_request(), profile("api"), CancellationContext()
            )
    assert caught.value.category is ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT
    assert caught.value.usage is not None and caught.value.usage.output_tokens == 7


@pytest.mark.anyio
@pytest.mark.parametrize("schema_version", ["independent-review.v1", "discussion-agent-reply.v1"])
async def test_grok_rejection_preserves_receipt(tmp_path: Path, schema_version: str) -> None:
    stdout = "\n".join(
        json.dumps(event)
        for event in (
            {"type": "text", "data": '{"unexpected":"field"}'},
            {
                "type": "end",
                "stopReason": "EndTurn",
                "requestId": "grok-receipt",
                "usage": {"input_tokens": 7, "output_tokens": 2},
            },
        )
    )
    configured = profile(
        "grok-a",
        provider="grok",
        launch_mode="cli",
        api_base_url=None,
        credential_reference=None,
        credential_required=False,
        runtime_identity="grok-a",
        state_directory=tmp_path / "grok-a",
    )
    with pytest.raises(ProviderFailure) as caught:
        await GrokCliAdapter(StaticCodexTransport(stdout)).execute(
            codex_request(schema=SCHEMA).model_copy(
                update={"output_schema_version": schema_version}
            ),
            configured,
            CancellationContext(),
        )
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == 7
    assert caught.value.usage.output_tokens == 2
    assert caught.value.provider_request_id == "grok-receipt"


def test_failure_receipt_is_immutable_and_unknown_usage_stays_unknown() -> None:
    failure = ProviderFailure(ProviderErrorCategory.PROVIDER_UNAVAILABLE, True, True)
    assert failure.receipt_result() is None
    usage = NormalizedUsage(input_tokens=42, duration_ms=1)
    measured = failure.with_receipt(usage, "receipt")
    assert measured.usage == usage and failure.usage is None
    assert not measured.execution_completed  # Usage alone does not certify completion.
    assert measured.with_receipt(NormalizedUsage(input_tokens=99, duration_ms=1)).usage == usage
    result = measured.receipt_result()
    assert result is not None and result.usage.output_tokens is None
