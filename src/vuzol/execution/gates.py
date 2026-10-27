"""System-owned trusted gates: finite command IDs run in the pinned sandbox."""

import hashlib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pydantic import Field

from vuzol.execution.domain import FrozenModel, ProcessEnvelope
from vuzol.execution.ports import SandboxRuntime
from vuzol.workflows.ports import CancellationContext


class RequiredGate(FrozenModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    command_id: str = Field(min_length=1, max_length=200)


TRUSTED_GATE_COMMANDS: dict[str, tuple[str, ...]] = {
    "vuzol secret-scan": ("/usr/local/bin/vuzol-secret-scan",),
    # The executable is resolved to the approved managed Node toolchain when
    # the validation sandbox envelope is built.
    "node --test": ("node", "--test"),
    "make build": ("/usr/bin/make", "build"),
    "make test": ("/usr/bin/make", "test"),
    "make format-check": ("/usr/bin/make", "format-check"),
    "make lint": ("/usr/bin/make", "lint"),
    "make type-check": ("/usr/bin/make", "type-check"),
    "make security": ("/usr/bin/make", "security"),
}
MANDATORY_SECURITY_GATE = RequiredGate(
    name="secret-scan",
    command_id="vuzol secret-scan",
)


class GateEvidence(FrozenModel):
    name: str
    command_id: str
    argv: tuple[str, ...]
    exit_code: int
    duration_ms: int = Field(ge=0)
    stdout_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stdout_bytes: int = Field(ge=0)
    stdout_truncated: bool
    stderr_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stderr_bytes: int = Field(ge=0)
    stderr_truncated: bool
    validation_image_digest: str | None = Field(
        default=None, pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$"
    )


@dataclass(frozen=True)
class CapturedOutput:
    content: bytes
    sha256: str
    byte_count: int
    truncated: bool


@dataclass(frozen=True)
class GateRun:
    evidence: GateEvidence
    stdout: CapturedOutput
    stderr: CapturedOutput


@dataclass(frozen=True)
class GateExecutionContext:
    task_id: uuid.UUID
    run_id: uuid.UUID
    step_id: uuid.UUID
    worktree_id: uuid.UUID
    profile_id: str
    provider_attempt: int
    lease_generation: int


class GateEnvelopePort(Protocol):
    async def build_gate(
        self,
        context: GateExecutionContext,
        argv: tuple[str, ...],
        *,
        timeout_seconds: int,
    ) -> ProcessEnvelope: ...


class GateRunner(Protocol):
    async def run(
        self,
        worktree: Path,
        gates: tuple[RequiredGate, ...],
        *,
        timeout_seconds: int,
        context: GateExecutionContext | None,
        cancellation: CancellationContext | None,
    ) -> tuple[GateRun, ...]: ...


class TrustedGateRunner:
    """Resolve finite command IDs to fixed argv in the pinned rootless sandbox."""

    def __init__(
        self,
        envelopes: GateEnvelopePort,
        runtime: SandboxRuntime,
    ) -> None:
        self._envelopes = envelopes
        self._runtime = runtime

    async def run(
        self,
        worktree: Path,
        gates: tuple[RequiredGate, ...],
        *,
        timeout_seconds: int,
        context: GateExecutionContext | None,
        cancellation: CancellationContext | None,
    ) -> tuple[GateRun, ...]:
        del worktree
        resolved: list[tuple[RequiredGate, tuple[str, ...]]] = []
        for gate in gates:
            argv = TRUSTED_GATE_COMMANDS.get(gate.command_id)
            if argv is None:
                raise ValueError(f"unknown trusted gate command ID: {gate.command_id}")
            resolved.append((gate, argv))
        if context is None or cancellation is None:
            raise ValueError("sandbox gate execution context is unavailable")
        return tuple(
            [
                await self._execute(
                    gate,
                    argv,
                    context=context,
                    cancellation=cancellation,
                    timeout_seconds=timeout_seconds,
                )
                for gate, argv in resolved
            ]
        )

    async def _execute(
        self,
        gate: RequiredGate,
        argv: tuple[str, ...],
        *,
        context: GateExecutionContext,
        cancellation: CancellationContext,
        timeout_seconds: int,
    ) -> GateRun:
        envelope = await self._envelopes.build_gate(
            context,
            argv,
            timeout_seconds=timeout_seconds,
        )
        result = await self._runtime.run(envelope, cancellation)
        stdout = _captured(result.stdout)
        stderr = _captured(result.stderr)
        evidence = GateEvidence(
            name=gate.name,
            command_id=gate.command_id,
            argv=argv,
            exit_code=result.exit_code,
            duration_ms=result.duration_ms,
            stdout_sha256=stdout.sha256,
            stdout_bytes=stdout.byte_count,
            stdout_truncated=stdout.truncated,
            stderr_sha256=stderr.sha256,
            stderr_bytes=stderr.byte_count,
            stderr_truncated=stderr.truncated,
            validation_image_digest=envelope.sandbox.image,
        )
        return GateRun(evidence=evidence, stdout=stdout, stderr=stderr)


def _captured(value: str) -> CapturedOutput:
    content = value.encode()
    return CapturedOutput(
        content=content,
        sha256=hashlib.sha256(content).hexdigest(),
        byte_count=len(content),
        truncated=False,
    )
