"""Replaceable workflow step execution boundary."""

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from vuzol.storage.records import LeaseToken
from vuzol.workflows.domain import StepOutcome


@dataclass(frozen=True, slots=True)
class StepExecutionRequest:
    task_id: uuid.UUID
    run_id: uuid.UUID
    step_id: uuid.UUID
    step_type: str
    payload: dict[str, Any]
    timeout_seconds: int
    lease: LeaseToken


class CancellationContext:
    def __init__(self) -> None:
        self._event = asyncio.Event()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()

    def request(self) -> None:
        self._event.set()


class StepHandler(Protocol):
    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome: ...
