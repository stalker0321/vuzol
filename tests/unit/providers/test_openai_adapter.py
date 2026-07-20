"""Openai adapter tests (split for cohesion)."""

from __future__ import annotations

from ._test_providers_helpers import (
    CancellationContext,
    OpenAICompatibleAdapter,
    ProviderErrorCategory,
    ProviderFailure,
    SecretStr,
    httpx,
    json,
    profile,
    provider_request,
    pytest,
)


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
