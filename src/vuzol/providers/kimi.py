"""Kimi Code CLI adapter for TokenRouter-backed coding execution."""

import json

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from vuzol.config.models import Capability, ProviderProfileConfig
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

KIMI_MODEL = "moonshotai/kimi-k3-free"


def canonical_kimi_argv(
    model: str,
    *,
    reasoning_effort: str = "low",
    read_only: bool = False,
) -> tuple[str, ...]:
    """Keep the potentially large task prompt on stdin and out of process metadata."""
    if model != KIMI_MODEL:
        raise ValueError("Kimi model is not allowlisted")
    if reasoning_effort not in {"low", "high", "max"}:
        raise ValueError("Kimi reasoning effort is not supported")
    plan = " --plan" if read_only else ""
    script = (
        'prompt="$(cat)"; exec kimi --auto --model tokenrouter/kimi-k3-free '
        f'--prompt "$prompt" --output-format stream-json{plan}'
    )
    mode = "plan" if read_only else "execute"
    return ("sh", "-c", script, f"vuzol-kimi-{mode}")


class KimiCliAdapter:
    adapter_version = "kimi-code-cli.v1"

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
                safe_summary="Kimi profile isolation is incomplete",
            )
        if request.sandbox_reference is None:
            raise ProviderFailure(
                ProviderErrorCategory.UNSUPPORTED_CAPABILITY,
                retryable=False,
                request_sent=False,
                safe_summary="Kimi execution requires an isolated worktree sandbox",
            )
        validator: Draft202012Validator | None = None
        if request.output_json_schema is not None:
            try:
                Draft202012Validator.check_schema(request.output_json_schema)
                validator = Draft202012Validator(request.output_json_schema)
            except SchemaError:
                raise ProviderFailure(
                    ProviderErrorCategory.PERMANENT_REQUEST,
                    retryable=False,
                    request_sent=False,
                    safe_summary="required output schema is invalid",
                ) from None
        prompt = json.dumps(
            {
                "schema_version": request.schema_version,
                "role": request.role.value,
                "original_input": request.original_input,
                "task_draft": request.task_draft,
                "context": [item.model_dump(mode="json") for item in request.context],
                "output_schema": request.output_json_schema,
                "execution_policy": "Inspect only; do not modify files."
                if Capability.CODE_EDIT not in request.required_capabilities
                else "Implement the requested changes and run relevant tests.",
            },
            ensure_ascii=False,
        )
        invocation = CodexInvocation(
            argv=canonical_kimi_argv(
                profile.model, read_only=Capability.CODE_EDIT not in request.required_capabilities
            ),
            stdin=prompt,
            runtime_identity=profile.runtime_identity,
            state_directory=str(profile.state_directory),
            timeout_seconds=request.timeout_seconds,
            sandbox_reference=request.sandbox_reference,
            task_id=request.task_id,
            run_id=request.run_id,
            step_id=request.step_id,
            profile_id=profile.id,
            provider_attempt=request.provider_attempt,
            lease_generation=request.lease_generation,
        )
        try:
            result = await self._transport.run(invocation, cancellation)
        except ValueError:
            raise
        except RuntimeError as error:
            timed_out = "timed out" in str(error).lower()
            raise ProviderFailure(
                ProviderErrorCategory.TIMEOUT
                if timed_out
                else ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary=(
                    "Kimi Code CLI timed out waiting for the provider"
                    if timed_out
                    else "supervised Kimi transport failed after launch was possible"
                ),
            ) from error
        if result.exit_code != 0:
            failure = f"{result.stdout}\n{result.stderr}".lower()
            if "connection_error" in failure or "connection error" in failure:
                category = ProviderErrorCategory.PROVIDER_UNAVAILABLE
                summary = "Kimi Code CLI could not connect to TokenRouter"
            elif "401" in failure or "authentication" in failure or "unauthorized" in failure:
                category = ProviderErrorCategory.AUTHENTICATION
                summary = "TokenRouter rejected Kimi authentication"
            elif "429" in failure or "rate limit" in failure:
                category = ProviderErrorCategory.RATE_LIMITED
                summary = "TokenRouter rate-limited Kimi"
            else:
                category = ProviderErrorCategory.PROVIDER_UNAVAILABLE
                summary = "Kimi Code CLI invocation failed"
            raise ProviderFailure(
                category,
                retryable=True,
                request_sent=True,
                safe_summary=summary,
            )
        try:
            decoded_text = _decode_output(result.stdout)
            text: str | None = decoded_text
            structured = None
            if validator is not None:
                decoded = json.loads(decoded_text)
                if not isinstance(decoded, dict):
                    raise ValueError("structured Kimi response is not an object")
                validator.validate(decoded)
                structured = decoded
                text = None
        except (json.JSONDecodeError, ValidationError, TypeError, ValueError):
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Kimi Code CLI returned invalid structured output",
            ) from None
        if not text and structured is None:
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Kimi Code CLI returned no final response",
            )
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=text,
            structured_output=structured,
            usage=NormalizedUsage(duration_ms=result.duration_ms),
            adapter_version=self.adapter_version,
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _decode_output(stdout: str) -> str:
    chunks: list[str] = []
    for line in stdout.splitlines():
        event = json.loads(line)
        if isinstance(event, dict) and event.get("role") == "assistant":
            content = event.get("content")
            if isinstance(content, str):
                chunks.append(content)
    if not chunks:
        raise ValueError("Kimi stream contains no assistant response")
    return "".join(chunks).strip()
