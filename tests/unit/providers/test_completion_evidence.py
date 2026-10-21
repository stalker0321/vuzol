"""Completion, transmission and usage are three independent provider facts."""

from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from tests.unit.providers._test_providers_helpers import (
    codex_profile,
    codex_request,
    profile,
    provider_request,
)
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.kimi import KIMI_MODEL, KimiCliAdapter
from vuzol.providers.openai import OpenAICompatibleAdapter
from vuzol.providers.pi import PiCliAdapter
from vuzol.providers.ports import CodexProcessResult
from vuzol.workflows.ports import CancellationContext


@pytest.mark.anyio
@pytest.mark.parametrize(
    "body", [b"not JSON", b'{"choices":[]}', b'{"choices":[{"message":{"content":"not JSON"}}]}']
)
async def test_fully_received_invalid_output_ends_call_without_inventing_usage(body: bytes) -> None:
    async with httpx.AsyncClient(
        base_url="https://provider.example/v1",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=body, headers={"x-request-id": "bad-output"})
        ),
    ) as client:
        with pytest.raises(ProviderFailure) as failure:
            await OpenAICompatibleAdapter(
                credential=SecretStr("fixture-only"), client=client
            ).execute(provider_request(structured=True), profile("api"), CancellationContext())
    assert failure.value.execution_completed and failure.value.request_sent
    assert failure.value.provider_request_id == "bad-output"
    assert failure.value.usage is None and failure.value.receipt_result() is None


@pytest.mark.anyio
@pytest.mark.parametrize("status", [200, 401, 429])
async def test_headers_without_complete_body_do_not_certify_completion(status: int) -> None:
    class InterruptedBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"partial":'
            raise httpx.ReadError("body interrupted")

    async with httpx.AsyncClient(
        base_url="https://provider.example/v1",
        transport=httpx.MockTransport(lambda _: httpx.Response(status, stream=InterruptedBody())),
    ) as client:
        with pytest.raises(ProviderFailure) as failure:
            await OpenAICompatibleAdapter(
                credential=SecretStr("fixture-only"), client=client
            ).execute(provider_request(), profile("api"), CancellationContext())
    assert failure.value.request_sent and not failure.value.execution_completed
    assert failure.value.usage is None


@pytest.mark.anyio
@pytest.mark.parametrize("provider", ["codex", "grok", "kimi", "pi"])
@pytest.mark.parametrize("outcome", ["transport-error", "bad-output", "nonzero-exit"])
async def test_cli_requires_returned_process_result_not_transport_exception(
    tmp_path: Path, provider: str, outcome: str
) -> None:
    class Transport:
        async def run(self, invocation, cancellation):
            if outcome == "transport-error":
                raise RuntimeError("uncertain launcher cleanup")
            return CodexProcessResult(
                exit_code=1 if outcome == "nonzero-exit" else 0,
                stdout="",
                stderr="",
                duration_ms=3,
            )

    adapter_type = {
        "codex": CodexCliAdapter,
        "grok": GrokCliAdapter,
        "kimi": KimiCliAdapter,
        "pi": PiCliAdapter,
    }[provider]
    with pytest.raises(ProviderFailure) as failure:
        await adapter_type(Transport()).execute(
            codex_request(),
            codex_profile(tmp_path).model_copy(
                update={
                    "provider": provider,
                    "model": KIMI_MODEL if provider == "kimi" else "model",
                }
            ),
            CancellationContext(),
        )
    assert failure.value.request_sent
    assert failure.value.execution_completed == (outcome != "transport-error")
