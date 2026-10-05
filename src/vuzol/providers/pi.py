"""Pi coding-agent CLI adapter (@earendil-works/pi-coding-agent 1.0.3).

Runs the one-shot ``pi -p --mode json`` path (stdin prompt -> JSONL events ->
EOF) through the existing sandbox transport, exactly like codex/kimi. The
terminal event is ``agent_settled``; ``agent_end`` is NOT terminal because
agent-level retries emit one ``agent_end`` per attempt before ``auto_retry_end``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from vuzol.config.models import Capability, ProviderProfileConfig
from vuzol.observability import get_logger
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

PI_PROVIDER = "opencode-go"
# Read-only surface for execute_agent: no bash/edit/write, only inspection tools.
PI_READ_ONLY_TOOLS = "read,grep,find,ls"
PI_SETTLED_EVENT = "agent_settled"

_logger = get_logger(__name__)


def canonical_pi_argv(model: str, *, read_only: bool = False) -> tuple[str, ...]:
    """Return the only Pi command accepted by the production sandbox transport."""

    resolved = (model or "").strip()
    if not resolved:
        raise ValueError("Pi model is required")
    args: list[str] = [
        "pi",
        "-p",
        "--mode",
        "json",
        "--no-session",
        "--provider",
        PI_PROVIDER,
        "--model",
        resolved,
    ]
    if read_only:
        args.extend(["--tools", PI_READ_ONLY_TOOLS])
    return tuple(args)


@dataclass(frozen=True, slots=True)
class PiDecoded:
    session_id: str | None
    text: str
    input_tokens: int | None
    output_tokens: int | None
    cached_tokens: int | None
    cost_units: Decimal | None
    stop_reason: str | None
    response_id: str | None
    retry_count: int


class PiOutputError(ValueError):
    """The Pi JSONL stream is incomplete or malformed."""


class PiCliAdapter:
    adapter_version = "pi-coding-agent-cli.v1"

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
                safe_summary="Pi profile isolation is incomplete",
            )
        if request.sandbox_reference is None:
            raise ProviderFailure(
                ProviderErrorCategory.UNSUPPORTED_CAPABILITY,
                retryable=False,
                request_sent=False,
                safe_summary="Pi execution requires an isolated worktree sandbox",
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
        read_only = Capability.CODE_EDIT not in request.required_capabilities
        envelope = {
            "schema_version": request.schema_version,
            "role": request.role.value,
            "original_input": request.original_input,
            "task_draft": request.task_draft,
            "context": [item.model_dump(mode="json") for item in request.context],
            "output_schema": request.output_json_schema,
            "execution_policy": "Inspect only; do not modify files."
            if read_only
            else "Implement the requested changes and run relevant tests.",
        }
        invocation = CodexInvocation(
            argv=canonical_pi_argv(profile.model, read_only=read_only),
            stdin=json.dumps(envelope, ensure_ascii=False),
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
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="supervised Pi transport failed after launch was possible",
            ) from error
        if result.exit_code != 0:
            failure = f"{result.stdout}\n{result.stderr}".lower()
            if "401" in failure or "unauthorized" in failure or "authentication" in failure:
                category = ProviderErrorCategory.AUTHENTICATION
                summary = "Pi was rejected by the provider authentication"
            elif "429" in failure or "rate limit" in failure:
                category = ProviderErrorCategory.RATE_LIMITED
                summary = "Pi was rate-limited by the provider"
            else:
                category = ProviderErrorCategory.PROVIDER_UNAVAILABLE
                summary = "Pi CLI invocation failed"
            raise ProviderFailure(category, retryable=True, request_sent=True, safe_summary=summary)
        try:
            decoded = _decode_pi_output(result.stdout)
        except PiOutputError as error:
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary=str(error),
            ) from None
        if decoded.retry_count:
            # Agent-level retries are visible in the stream; log them, never
            # treat an intermediate agent_end as the final answer.
            _logger.info(
                "provider.pi.retries",
                extra={
                    "event": "provider.pi.retries",
                    "attempts": decoded.retry_count,
                    "profile_id": profile.id,
                },
            )
        stop_reason = decoded.stop_reason
        if stop_reason == "aborted":
            raise ProviderFailure(
                ProviderErrorCategory.CANCELLED,
                retryable=False,
                request_sent=True,
                safe_summary="Pi request was aborted",
            )
        if stop_reason == "error":
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="Pi agent stopped with an error",
            )
        text: str | None = decoded.text
        structured: dict[str, Any] | None = None
        if validator is not None:
            try:
                decoded_json = json.loads(decoded.text)
                if not isinstance(decoded_json, dict):
                    raise ValueError("structured Pi response is not an object")
                validator.validate(decoded_json)
            except (json.JSONDecodeError, JsonSchemaValidationError, ValueError):
                raise ProviderFailure(
                    ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                    retryable=True,
                    request_sent=True,
                    safe_summary="Pi returned invalid structured output",
                ) from None
            structured = decoded_json
            text = None
        if not text and structured is None:
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Pi returned no final response",
            )
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=text,
            structured_output=structured,
            provider_request_id=decoded.response_id,
            provider_session_id=decoded.session_id,
            usage=NormalizedUsage(
                input_tokens=decoded.input_tokens,
                output_tokens=decoded.output_tokens,
                cached_tokens=decoded.cached_tokens,
                cost_units=decoded.cost_units,
                duration_ms=result.duration_ms,
            ),
            finish_reason=stop_reason,
            adapter_version=self.adapter_version,
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _optional_int(value: object) -> int | None:
    return int(value) if isinstance(value, int | float) and value >= 0 else None


def _optional_decimal(value: object) -> Decimal | None:
    if not isinstance(value, int | float):
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        return None
    return result if result >= 0 else None


def _assistant_text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block["text"]
            for block in content
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        )
    return ""


def _decode_pi_output(stdout: str) -> PiDecoded:
    """Parse the ``-p --mode json`` stream: final answer is bound to agent_settled."""

    events: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    if not events:
        raise PiOutputError("Pi returned an empty event stream")
    settled = any(event.get("type") == PI_SETTLED_EVENT for event in events)
    if not settled:
        raise PiOutputError("Pi stream ended without agent_settled")
    session_id: str | None = None
    retry_count = 0
    final_message: dict[str, Any] | None = None
    for event in events:
        event_type = event.get("type")
        if event_type == "session" and isinstance(event.get("id"), str):
            session_id = event["id"]
        elif event_type == "auto_retry_start":
            retry_count += 1
        elif event_type == "agent_end":
            messages = event.get("messages")
            if isinstance(messages, list):
                for message in messages:
                    if isinstance(message, dict) and message.get("role") == "assistant":
                        final_message = message
    if final_message is None:
        raise PiOutputError("Pi stream has no assistant message")
    usage = final_message.get("usage")
    usage = usage if isinstance(usage, dict) else {}
    cost = usage.get("cost")
    cost_total = cost.get("total") if isinstance(cost, dict) else None
    stop_reason = final_message.get("stopReason")
    if not isinstance(stop_reason, str):
        stop_reason = None
    response_id = final_message.get("responseId")
    return PiDecoded(
        session_id=session_id,
        text=_assistant_text(final_message.get("content")).strip(),
        input_tokens=_optional_int(usage.get("input")),
        output_tokens=_optional_int(usage.get("output")),
        cached_tokens=_optional_int(usage.get("cacheRead")),
        cost_units=_optional_decimal(cost_total),
        stop_reason=stop_reason,
        response_id=response_id if isinstance(response_id, str) else None,
        retry_count=retry_count,
    )
