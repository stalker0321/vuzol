"""Durable read-only research job contracts, independent of work packages."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from vuzol.discussion.domain import DomainError  # noqa: F401
from vuzol.leadership.persistence import LIVE_GOAL_LIFECYCLES, LeadJob, ManagedGoal

RESULT_BYTES = 16 * 1024
_TRUNCATION = "\n[truncated]"
NONTERMINAL_STATES = ("queued", "running")


class ResearchInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)

    question: str = Field(min_length=1, max_length=2000)


@dataclass(frozen=True, slots=True)
class JobClaim:
    job_id: uuid.UUID
    goal_id: uuid.UUID
    owner: str
    generation: int


def bounded_report(text: str) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= RESULT_BYTES:
        return text
    return (
        encoded[: RESULT_BYTES - len(_TRUNCATION.encode())].decode("utf-8", errors="ignore")
        + _TRUNCATION
    )


def claim_matches(job: LeadJob, goal: ManagedGoal, claim: JobClaim, *, now: datetime) -> bool:
    """A lead turn may advance independently; this authority belongs to the job."""
    return (
        job.id == claim.job_id
        and job.goal_id == goal.id == claim.goal_id
        and goal.lifecycle in LIVE_GOAL_LIFECYCLES
        and job.state == "running"
        and job.lease_owner == claim.owner
        and job.lease_generation == claim.generation
        and job.lease_expires_at is not None
        and job.lease_expires_at > now
    )


def job_view(job: LeadJob, *, current_head: str | None) -> dict[str, Any]:
    return {
        "id": str(job.id),
        "kind": job.kind,
        "state": job.state,
        "question": job.input["question"][:200],
        "pinned_commit": job.pinned_commit,
        "stale": job.pinned_commit != current_head,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }
