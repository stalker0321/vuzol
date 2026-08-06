"""Kimi Code CLI adapter for TokenRouter-backed coding execution."""

import json
import stat
from pathlib import Path

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
_MAX_WIRE_APPEND_BYTES = 16 * 1024 * 1024


def canonical_kimi_argv(model: str, *, read_only: bool = False) -> tuple[str, ...]:
    """Keep the potentially large task prompt on stdin and out of process metadata."""
    if model != KIMI_MODEL:
        raise ValueError("Kimi model is not allowlisted")
    script = (
        'prompt="$(cat)"; exec kimi --model tokenrouter/kimi-k3-free '
        '--prompt "$prompt" --output-format stream-json'
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
        usage_snapshot = _snapshot_wire_usage(Path(profile.state_directory))
        try:
            result = await self._transport.run(invocation, cancellation)
        except ValueError:
            raise
        except RuntimeError as error:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="supervised Kimi transport failed after launch was possible",
                usage=_read_wire_usage(Path(profile.state_directory), usage_snapshot, 0),
            ) from error
        usage = _read_wire_usage(Path(profile.state_directory), usage_snapshot, result.duration_ms)
        if result.exit_code != 0:
            raise ProviderFailure(
                ProviderErrorCategory.PROVIDER_UNAVAILABLE,
                retryable=True,
                request_sent=True,
                safe_summary="Kimi Code CLI invocation failed",
                usage=usage,
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
            usage=usage or NormalizedUsage(duration_ms=result.duration_ms),
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


def _snapshot_wire_usage(root: Path) -> dict[Path, int]:
    return {path: path.stat().st_size for path in _wire_files(root)}


def _read_wire_usage(
    root: Path, snapshot: dict[Path, int], duration_ms: int
) -> NormalizedUsage | None:
    input_tokens = 0
    cached_tokens = 0
    output_tokens = 0
    found = False
    for path in _wire_files(root):
        offset = snapshot.get(path, 0)
        size = path.stat().st_size
        if size < offset or size - offset > _MAX_WIRE_APPEND_BYTES:
            continue
        with path.open("rb") as stream:
            stream.seek(offset)
            for raw_line in stream:
                try:
                    event = json.loads(raw_line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(event, dict) or event.get("type") != "usage.record":
                    continue
                usage = event.get("usage")
                if not isinstance(usage, dict):
                    continue
                values = {
                    key: _nonnegative_int(usage.get(key))
                    for key in ("inputOther", "inputCacheRead", "inputCacheCreation", "output")
                }
                if any(value is None for value in values.values()):
                    continue
                found = True
                cached_tokens += values["inputCacheRead"] or 0
                input_tokens += sum(
                    values[key] or 0
                    for key in ("inputOther", "inputCacheRead", "inputCacheCreation")
                )
                output_tokens += values["output"] or 0
    if not found:
        return None
    return NormalizedUsage(
        input_tokens=input_tokens,
        cached_tokens=cached_tokens,
        output_tokens=output_tokens,
        duration_ms=duration_ms,
    )


def _wire_files(root: Path) -> tuple[Path, ...]:
    sessions = root / "sessions"
    if not sessions.is_dir() or sessions.is_symlink():
        return ()
    files: list[Path] = []
    for path in sessions.glob("*/*/agents/main/wire.jsonl"):
        try:
            metadata = path.lstat()
        except OSError:
            continue
        if stat.S_ISREG(metadata.st_mode) and not path.is_symlink():
            files.append(path)
    return tuple(sorted(files))


def _nonnegative_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None
