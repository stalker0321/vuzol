"""Codex CLI adapter contract; production sandbox transport arrives in Step 08."""

import json

from vuzol.config.models import ProviderProfileConfig
from vuzol.providers.domain import (
    EffectiveProfileState,
    NormalizedUsage,
    ProviderErrorCategory,
    ProviderRequest,
    ProviderResult,
    ProviderResultStatus,
)
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.ports import CodexInvocation, CodexProcessTransport
from vuzol.workflows.ports import CancellationContext


class CodexCliAdapter:
    adapter_version = "codex-cli.v1"

    def __init__(self, transport: CodexProcessTransport) -> None:
        self._transport = transport

    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult:
        if profile.runtime_identity is None or profile.state_directory is None:
            raise ProviderFailure(
                ProviderErrorCategory.PERMANENT_REQUEST,
                retryable=False,
                request_sent=False,
                safe_summary="Codex profile isolation is incomplete",
            )
        if request.sandbox_reference is not None:
            raise ProviderFailure(
                ProviderErrorCategory.UNSUPPORTED_CAPABILITY,
                retryable=False,
                request_sent=False,
                safe_summary="Step 08 sandbox transport is not installed",
            )
        envelope = {
            "schema_version": request.schema_version,
            "role": request.role.value,
            "original_input": request.original_input,
            "task_draft": request.task_draft,
            "context": [item.model_dump(mode="json") for item in request.context],
            "output_schema": request.output_json_schema,
        }
        invocation = CodexInvocation(
            argv=("codex", "exec", "--json", "--sandbox", "read-only", "-"),
            stdin=json.dumps(envelope, ensure_ascii=False),
            runtime_identity=profile.runtime_identity,
            state_directory=str(profile.state_directory),
            timeout_seconds=request.timeout_seconds,
        )
        result = await self._transport.run(invocation, cancellation)
        if result.exit_code != 0:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="Codex CLI invocation failed",
            )
        try:
            body = json.loads(result.stdout)
            text = body.get("text")
            structured = body.get("structured_output")
            usage = body.get("usage", {})
        except (json.JSONDecodeError, AttributeError) as error:
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Codex CLI returned invalid output",
            ) from error
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=str(text) if text is not None else None,
            structured_output=structured if isinstance(structured, dict) else None,
            provider_request_id=(
                str(body["request_id"]) if isinstance(body.get("request_id"), str) else None
            ),
            provider_session_id=(
                str(body["session_id"]) if isinstance(body.get("session_id"), str) else None
            ),
            usage=NormalizedUsage(
                input_tokens=_usage_int(usage, "input_tokens"),
                output_tokens=_usage_int(usage, "output_tokens"),
                cached_tokens=_usage_int(usage, "cached_tokens"),
                duration_ms=result.duration_ms,
            ),
            finish_reason=(
                str(body["finish_reason"]) if isinstance(body.get("finish_reason"), str) else None
            ),
            adapter_version=self.adapter_version,
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _usage_int(usage: object, key: str) -> int | None:
    if not isinstance(usage, dict):
        return None
    value = usage.get(key)
    return int(value) if isinstance(value, int | float) and value >= 0 else None
