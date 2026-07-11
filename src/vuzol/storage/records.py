"""Detached records returned beyond repository boundaries."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from vuzol.storage.types import DeliveryStatus, StepStatus, TaskStatus


@dataclass(frozen=True, slots=True)
class TaskRecord:
    id: uuid.UUID
    status: TaskStatus
    original_text: str
    task_draft: dict[str, Any]
    version: int


@dataclass(frozen=True, slots=True)
class StepRecord:
    id: uuid.UUID
    run_id: uuid.UUID
    status: StepStatus
    lease_generation: int
    lease_owner: str | None
    lease_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeaseToken:
    step: StepRecord
    owner: str
    generation: int


@dataclass(frozen=True, slots=True)
class OutboxLeaseToken:
    item_id: uuid.UUID
    status: DeliveryStatus
    owner: str
    generation: int
    lease_expires_at: datetime
