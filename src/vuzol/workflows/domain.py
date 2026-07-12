"""Provider-neutral immutable workflow contracts."""

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from vuzol.config import Capability
from vuzol.storage.types import IdempotencyClass, QueueClass, RetryClass, StepStatus


class WorkflowDefinitionError(ValueError):
    """A code-defined workflow is internally inconsistent."""


class OutcomeKind(StrEnum):
    SUCCEEDED = "succeeded"
    TRANSIENT_FAILURE = "transient_failure"
    PERMANENT_FAILURE = "permanent_failure"
    NEEDS_USER_INPUT = "needs_user_input"
    NEEDS_APPROVAL = "needs_approval"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class StepDefinition:
    key: str
    step_type: str
    predecessors: tuple[str, ...]
    queue_class: QueueClass
    capabilities: frozenset[Capability]
    retry_class: RetryClass
    idempotency_class: IdempotencyClass
    timeout_seconds: int
    max_attempts: int
    priority: int = 100
    optional_flag: str | None = None
    internal: bool = False


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    workflow_type: str
    version: str
    task_types: frozenset[str]
    steps: tuple[StepDefinition, ...]

    @property
    def stable_id(self) -> str:
        return f"{self.workflow_type}.v{self.version}"


@dataclass(frozen=True, slots=True)
class MaterializedStep:
    ordinal: int
    key: str
    step_type: str
    predecessor_ordinals: tuple[int, ...]
    queue_class: QueueClass
    capabilities: frozenset[Capability]
    retry_class: RetryClass
    idempotency_class: IdempotencyClass
    timeout_seconds: int
    max_attempts: int
    priority: int
    status: StepStatus = StepStatus.PENDING
    payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class MaterializedWorkflow:
    workflow_type: str
    version: str
    interpretation_id: uuid.UUID
    steps: tuple[MaterializedStep, ...]


@dataclass(frozen=True, slots=True)
class StepOutcome:
    kind: OutcomeKind
    result: dict[str, Any]
    category: str | None = None
    summary: str | None = None
    unknown_effects: bool = False

    @classmethod
    def succeeded(cls, result: dict[str, Any] | None = None) -> "StepOutcome":
        return cls(kind=OutcomeKind.SUCCEEDED, result=dict(result or {}))
