"""Replaceable provider adapter and Codex transport ports."""

import uuid
from dataclasses import dataclass
from typing import Protocol

from vuzol.config.models import ProviderProfileConfig
from vuzol.providers.domain import EffectiveProfileState, ProviderRequest, ProviderResult
from vuzol.workflows.ports import CancellationContext


class ProviderAdapter(Protocol):
    async def execute(
        self,
        request: ProviderRequest,
        profile: ProviderProfileConfig,
        cancellation: CancellationContext,
    ) -> ProviderResult: ...

    async def health(self, profile: ProviderProfileConfig) -> EffectiveProfileState: ...


@dataclass(frozen=True, slots=True)
class CodexInvocation:
    argv: tuple[str, ...]
    stdin: str
    runtime_identity: str
    state_directory: str
    timeout_seconds: float
    sandbox_reference: str | None = None
    task_id: uuid.UUID | None = None
    run_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    profile_id: str | None = None
    provider_attempt: int | None = None
    lease_generation: int | None = None


@dataclass(frozen=True, slots=True)
class CodexProcessResult:
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int


class CodexProcessTransport(Protocol):
    async def run(
        self, invocation: CodexInvocation, cancellation: CancellationContext
    ) -> CodexProcessResult: ...
