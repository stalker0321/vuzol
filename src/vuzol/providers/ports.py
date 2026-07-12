"""Replaceable provider adapter and Codex transport ports."""

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
