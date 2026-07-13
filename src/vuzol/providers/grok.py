"""Grok Build CLI adapter for subscription-backed sandbox execution."""

import hashlib
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


def canonical_grok_argv(model: str) -> tuple[str, ...]:
    """Return the only Grok command accepted by the production transport."""
    return (
        "grok",
        "--no-auto-update",
        "--prompt-file",
        "/dev/stdin",
        "--output-format",
        "streaming-json",
        "--cwd",
        "/workspace",
        "--model",
        model,
        "--permission-mode",
        "dontAsk",
        "--sandbox",
        "strict",
        "--allow",
        "Bash(git *)",
        "--allow",
        "Bash(make *)",
        "--allow",
        "Bash(./verify.sh*)",
        "--allow",
        "Edit(**)",
        "--allow",
        "Read(**)",
        "--allow",
        "Grep(**)",
        "--deny",
        "Read(/grok-home/**)",
        "--deny",
        "Edit(/grok-home/**)",
        "--deny",
        "Bash(git push*)",
        "--deny",
        "Bash(git reset*)",
        "--deny",
        "Bash(git clean*)",
        "--no-memory",
        "--no-subagents",
        "--disable-web-search",
        "--verbatim",
    )


class GrokCliAdapter:
    adapter_version = "grok-cli.v1"

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
                safe_summary="Grok profile isolation is incomplete",
            )
        if request.sandbox_reference is None:
            raise ProviderFailure(
                ProviderErrorCategory.UNSUPPORTED_CAPABILITY,
                retryable=False,
                request_sent=False,
                safe_summary="Grok execution requires an isolated worktree sandbox",
            )
        prompt = json.dumps(
            {
                "schema_version": request.schema_version,
                "role": request.role.value,
                "original_input": request.original_input,
                "task_draft": request.task_draft,
                "context": [item.model_dump(mode="json") for item in request.context],
                "output_schema": request.output_json_schema,
                "execution_policy": {
                    "shell_invocation": (
                        "Invoke each allowed shell command separately; do not use cd, chains, "
                        "wrappers, or command substitution. Commands must begin exactly with "
                        "git, make, or ./verify.sh."
                    ),
                    "file_edits": "Use the Edit tool for workspace file changes.",
                },
            },
            ensure_ascii=False,
        )
        invocation = CodexInvocation(
            argv=canonical_grok_argv(profile.model),
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
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="supervised Grok transport failed after launch was possible",
            ) from error
        if result.exit_code != 0:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="Grok CLI invocation failed",
            )
        try:
            body = _decode_output(result.stdout)
            usage = body.get("usage", {})
        except GrokProviderCancelled as error:
            raise ProviderFailure(
                ProviderErrorCategory.CANCELLED,
                retryable=False,
                request_sent=True,
                safe_summary="Grok CLI reported provider-originated cancellation",
            ) from error
        except (json.JSONDecodeError, ValueError) as error:
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Grok CLI returned invalid output",
            ) from error
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=str(body["text"]),
            provider_request_id=_optional_string(body.get("request_id")),
            provider_session_id=_optional_string(body.get("session_id")),
            usage=NormalizedUsage(
                input_tokens=_usage_int(usage, "input_tokens"),
                output_tokens=_usage_int(usage, "output_tokens"),
                cached_tokens=_usage_int(usage, "cache_read_input_tokens"),
                duration_ms=result.duration_ms,
            ),
            finish_reason=_optional_string(body.get("finish_reason")),
            adapter_version=self.adapter_version,
        )

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState:
        del profile
        return EffectiveProfileState()


def _decode_output(stdout: str) -> dict[str, object]:
    chunks: list[str] = []
    final: dict[str, object] | None = None
    for line in stdout.splitlines():
        event = json.loads(line)
        if not isinstance(event, dict):
            continue
        if event.get("type") == "text" and isinstance(event.get("data"), str):
            chunks.append(event["data"])
        elif event.get("type") == "end":
            final = event
    if final is not None and final.get("stopReason") == "Cancelled":
        raise GrokProviderCancelled("Grok JSONL ended with provider cancellation")
    if final is None or not chunks:
        raise ValueError("Grok JSONL contains no completed response")
    return {
        "text": "".join(chunks),
        "request_id": final.get("requestId"),
        "session_id": final.get("sessionId"),
        "finish_reason": final.get("stopReason"),
        "usage": final.get("usage", {}),
    }


class GrokProviderCancelled(ValueError):
    """The CLI completed normally but its structured protocol reported cancellation."""


def summarize_grok_events(stdout: str) -> dict[str, object]:
    """Return a prompt- and output-free diagnostic summary of Grok JSONL."""
    events: list[dict[str, object]] = []
    for sequence, line in enumerate(stdout.splitlines(), start=1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        event_type = event.get("type")
        if not isinstance(event_type, str):
            continue
        encoded = line.encode()
        events.append(
            {
                "sequence": sequence,
                "type": event_type,
                "byte_count": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
                "stop_reason": event.get("stopReason") if event_type == "end" else None,
            }
        )
    final = events[-1] if events else None
    return {
        "schema_version": "grok-event-summary.v1",
        "event_count": len(events),
        "events": events,
        "last_event_type": final["type"] if final else None,
        "last_stop_reason": final["stop_reason"] if final else None,
    }


def _usage_int(usage: object, key: str) -> int | None:
    if not isinstance(usage, dict):
        return None
    value = usage.get(key)
    return int(value) if isinstance(value, int | float) and value >= 0 else None


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
