"""Codex CLI adapter contract; production sandbox transport arrives in Step 08."""

import json

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

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

CODEX_PERMISSION_PROFILE = "vuzol-executor"
CODEX_PERMISSION_CONFIG = (
    'permissions.vuzol-executor={filesystem={":minimal"="read",'
    '"/workspace"="write","/codex-home"="none"},'
    "network={enabled=false}}"
)
CODEX_READ_ONLY_PERMISSION_PROFILE = "vuzol-reader"
CODEX_READ_ONLY_PERMISSION_CONFIG = (
    'permissions.vuzol-reader={filesystem={":minimal"="read",'
    '"/workspace"="read","/codex-home"="none"},'
    "network={enabled=false}}"
)


def canonical_codex_argv(
    *,
    model: str | None = None,
    reasoning_effort: str | None = None,
    read_only: bool = False,
) -> tuple[str, ...]:
    """Return the only Codex command accepted by the production sandbox transport.

    When ``model`` is a real Codex slug (e.g. ``gpt-5.6-sol``), pass it explicitly
    so the dashboard and usage records can report the full executor identity.
    The generic registry token ``codex`` is left unset so the CLI default applies.
    """
    permission_profile = (
        CODEX_READ_ONLY_PERMISSION_PROFILE if read_only else CODEX_PERMISSION_PROFILE
    )
    permission_config = CODEX_READ_ONLY_PERMISSION_CONFIG if read_only else CODEX_PERMISSION_CONFIG
    args: list[str] = [
        "codex",
        "exec",
        "--json",
        "--strict-config",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
    ]
    resolved_model = (model or "").strip()
    if resolved_model and resolved_model.lower() not in {"codex", "auto"}:
        args.extend(["--model", resolved_model])
    if isinstance(reasoning_effort, str) and reasoning_effort.strip():
        args.extend(["--config", f'model_reasoning_effort="{reasoning_effort.strip()}"'])
    args.extend(
        [
            "--config",
            'approval_policy="never"',
            "--config",
            f'default_permissions="{permission_profile}"',
            "--config",
            permission_config,
            "--cd",
            "/workspace",
            "-",
        ]
    )
    return tuple(args)


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
        if request.sandbox_reference is None:
            raise ProviderFailure(
                ProviderErrorCategory.UNSUPPORTED_CAPABILITY,
                retryable=False,
                request_sent=False,
                safe_summary="Codex execution requires an isolated worktree sandbox",
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
        envelope = {
            "schema_version": request.schema_version,
            "role": request.role.value,
            "original_input": request.original_input,
            "task_draft": request.task_draft,
            "context": [item.model_dump(mode="json") for item in request.context],
            "output_schema": request.output_json_schema,
        }
        invocation = CodexInvocation(
            argv=canonical_codex_argv(
                model=profile.model,
                reasoning_effort=profile.model_reasoning_effort,
                read_only=Capability.CODE_EDIT not in request.required_capabilities,
            ),
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
                safe_summary="supervised Codex transport failed after launch was possible",
            ) from error
        if result.exit_code != 0:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="Codex CLI invocation failed",
            )
        try:
            body = _decode_output(result.stdout)
            text, structured = _normalize_output(body, validator)
            usage = body.get("usage", {})
        except (
            json.JSONDecodeError,
            JsonSchemaValidationError,
            AttributeError,
            TypeError,
            ValueError,
        ):
            raise ProviderFailure(
                ProviderErrorCategory.INVALID_STRUCTURED_OUTPUT,
                retryable=True,
                request_sent=True,
                safe_summary="Codex CLI returned invalid structured output",
            ) from None
        return ProviderResult(
            status=ProviderResultStatus.SUCCEEDED,
            text=text,
            structured_output=structured,
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


def _normalize_output(
    body: dict[str, object], validator: Draft202012Validator | None
) -> tuple[str | None, dict[str, object] | None]:
    """Normalize the final response without heuristic JSON extraction."""
    text = body.get("text")
    if validator is None:
        return (str(text) if text is not None else None), None

    direct = body.get("structured_output")
    if direct is not None:
        decoded = direct
    else:
        if not isinstance(text, str):
            raise ValueError("structured Codex response has no final text")
        decoded = json.loads(text)
    if not isinstance(decoded, dict):
        raise ValueError("structured Codex response is not an object")
    validator.validate(decoded)
    return None, decoded


def _decode_output(stdout: str) -> dict[str, object]:
    try:
        decoded = json.loads(stdout)
    except json.JSONDecodeError:
        decoded = None
    if isinstance(decoded, dict):
        return decoded
    final: dict[str, object] = {}
    usage: dict[str, object] = {}
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        if event.get("type") == "thread.started" and isinstance(event.get("thread_id"), str):
            final["session_id"] = event["thread_id"]
        if event.get("type") == "item.completed" and isinstance(event.get("item"), dict):
            item = event["item"]
            if item.get("type") == "agent_message" and isinstance(item.get("text"), str):
                final["text"] = item["text"]
        if event.get("type") == "turn.completed" and isinstance(event.get("usage"), dict):
            usage = event["usage"]
    if not final:
        raise ValueError("Codex JSONL contains no final agent message")
    final["usage"] = usage
    return final
