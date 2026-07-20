"""Codex adapter tests (split for cohesion)."""

from __future__ import annotations

from ._test_providers_helpers import (
    CancellationContext,
    CodexCliAdapter,
    CodexInvocation,
    CodexProcessResult,
    FakeCodexTransport,
    LaunchMode,
    NormalizedUsage,
    Path,
    ProviderErrorCategory,
    ProviderFailure,
    StaticCodexTransport,
    WorkerEditReport,
    codex_jsonl,
    codex_profile,
    codex_request,
    json,
    profile,
    provider_request,
    pytest,
)


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
