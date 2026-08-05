"""Pure work-package values, validation, transitions, and event vocabulary."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from vuzol.storage.types import EstimatedComplexity, RiskLevel, WorkPackageStatus

_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
TERMINAL_PACKAGE_STATUSES = frozenset(
    {WorkPackageStatus.COMPLETED, WorkPackageStatus.STOPPED, WorkPackageStatus.DISCARDED}
)


class WorkPackageEvent(StrEnum):
    PACKAGE_CREATED = "work_package.created"
    REVISION_CREATED = "plan_revision.created"
    REVISION_APPROVED = "plan_revision.approved"
    REVISION_SUPERSEDED = "plan_revision.superseded"
    PACKAGE_DISCARDED = "work_package.discarded"
    PACKAGE_PAUSED = "work_package.paused"
    PACKAGE_RETRIED = "work_package.item_retry_requested"
    PACKAGE_ITEM_SKIPPED = "work_package.item_skipped"
    PACKAGE_STOPPED = "work_package.stopped"
    PACKAGE_REPLAN_REQUESTED = "work_package.replan_requested"
    PACKAGE_STARTED = "work_package.started"
    PACKAGE_ITEM_MATERIALIZED = "work_package.item_materialized"
    PACKAGE_COMPLETED = "work_package.completed"
    DETAIL_POINTER_CHANGED = "work_package.detail_pointer_changed"
    EDIT_SESSION_OPENED = "edit_session.opened"
    EDIT_SESSION_CLOSED = "edit_session.closed"
    EDIT_SESSION_ACCEPTED = "edit_session.accepted"
    EDIT_SESSION_EXPIRED = "edit_session.expired"


class PackageControlAction(StrEnum):
    APPROVE = "approve"
    START = "start"
    DISCARD = "discard"
    RETRY_ITEM = "retry_item"
    SKIP_ITEM = "skip_item"
    STOP_PACKAGE = "stop_package"
    REQUEST_REPLAN = "request_replan"


class DomainError(RuntimeError):
    """Stable, fail-closed domain rejection suitable for application mapping."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class PlanItemDraft:
    summary: str
    goal: str
    expected_outcome: str
    completion_criteria: tuple[str, ...]
    allowed_scope: str
    item_id: uuid.UUID | None = None
    local_id: str | None = None
    out_of_scope: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    trusted_checks: tuple[str, ...] = ()
    suggested_risk: RiskLevel = RiskLevel.LOW
    needs_approval: bool = False
    estimated_complexity: EstimatedComplexity = EstimatedComplexity.MEDIUM

    def __post_init__(self) -> None:
        if not self.summary.strip() or len(self.summary) > 240:
            raise DomainError("invalid_plan", "item summary must contain 1..240 characters")
        for name, value in (
            ("goal", self.goal),
            ("expected_outcome", self.expected_outcome),
            ("allowed_scope", self.allowed_scope),
        ):
            if not value.strip():
                raise DomainError("invalid_plan", f"item {name} is required")
        if not self.completion_criteria or any(
            not value.strip() for value in self.completion_criteria
        ):
            raise DomainError("invalid_plan", "completion criteria must contain non-empty values")
        if self.local_id is not None and (
            len(self.local_id) > 64 or _SLUG.fullmatch(self.local_id) is None
        ):
            raise DomainError("invalid_plan", "item local_id must be a lowercase slug")


@dataclass(frozen=True, slots=True)
class PlanDraft:
    title: str
    items: tuple[PlanItemDraft, ...]

    def __post_init__(self) -> None:
        if not self.title.strip() or len(self.title) > 240:
            raise DomainError("invalid_plan", "package title must contain 1..240 characters")
        if not self.items:
            raise DomainError("invalid_plan", "a plan needs at least one item")
        local_ids = [item.local_id for item in self.items if item.local_id is not None]
        if len(local_ids) != len(set(local_ids)):
            raise DomainError("invalid_plan", "item local_id values must be unique")
        item_ids = [item.item_id for item in self.items if item.item_id is not None]
        if len(item_ids) != len(set(item_ids)):
            raise DomainError("invalid_plan", "item_id values must be unique")


def canonical_plan_body(plan: PlanDraft, item_ids: tuple[uuid.UUID, ...]) -> dict[str, Any]:
    if len(plan.items) != len(item_ids):
        raise DomainError("invalid_plan", "resolved item identities do not match plan items")
    return {
        "title": plan.title.strip(),
        "items": [
            {
                "item_id": str(item_id),
                "local_id": item.local_id,
                "ordinal": ordinal,
                "summary": item.summary.strip(),
                "goal": item.goal.strip(),
                "expected_outcome": item.expected_outcome.strip(),
                "completion_criteria": list(item.completion_criteria),
                "allowed_scope": item.allowed_scope.strip(),
                "out_of_scope": list(item.out_of_scope),
                "dependencies": list(item.dependencies),
                "trusted_checks": list(item.trusted_checks),
                "suggested_risk": item.suggested_risk.value,
                "needs_approval": item.needs_approval,
                "estimated_complexity": item.estimated_complexity.value,
            }
            for ordinal, (item, item_id) in enumerate(zip(plan.items, item_ids, strict=True), 1)
        ],
    }


def canonical_plan_hash(body: dict[str, Any]) -> str:
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def semantic_plan_hash(plan: PlanDraft) -> str:
    """Fingerprint user-visible plan meaning without package-local identities."""

    body = {
        "title": plan.title.strip(),
        "items": [
            {
                "local_id": item.local_id,
                "summary": item.summary.strip(),
                "goal": item.goal.strip(),
                "expected_outcome": item.expected_outcome.strip(),
                "completion_criteria": list(item.completion_criteria),
                "allowed_scope": item.allowed_scope.strip(),
                "out_of_scope": list(item.out_of_scope),
                "dependencies": list(item.dependencies),
                "trusted_checks": list(item.trusted_checks),
                "suggested_risk": item.suggested_risk.value,
                "needs_approval": item.needs_approval,
                "estimated_complexity": item.estimated_complexity.value,
            }
            for item in plan.items
        ],
    }
    return canonical_plan_hash(body)


def semantic_revision_hash(body: dict[str, Any]) -> str:
    """Fingerprint a stored canonical revision using the same semantic projection."""

    raw_items = body.get("items")
    if not isinstance(raw_items, list):
        raise DomainError("invalid_plan", "stored plan body has no items")
    projected = {
        "title": str(body.get("title", "")).strip(),
        "items": [
            {key: value for key, value in item.items() if key not in {"item_id", "ordinal"}}
            for item in raw_items
            if isinstance(item, dict)
        ],
    }
    return canonical_plan_hash(projected)


def plan_outline_hash(plan: PlanDraft) -> str:
    """Fingerprint the stable user-visible outline, ignoring model paraphrases."""

    return canonical_plan_hash(
        {
            "title": _normalized_outline_text(plan.title),
            "items": [_normalized_outline_text(item.summary) for item in plan.items],
        }
    )


def revision_outline_hash(body: dict[str, Any]) -> str:
    """Project a stored revision onto the same stable outline fingerprint."""

    raw_items = body.get("items")
    if not isinstance(raw_items, list):
        raise DomainError("invalid_plan", "stored plan body has no items")
    summaries = [
        _normalized_outline_text(str(item.get("summary", "")))
        for item in raw_items
        if isinstance(item, dict)
    ]
    return canonical_plan_hash(
        {
            "title": _normalized_outline_text(str(body.get("title", ""))),
            "items": summaries,
        }
    )


def _normalized_outline_text(value: str) -> str:
    return " ".join(value.casefold().split())


def require_mutable(status: WorkPackageStatus) -> None:
    if status in TERMINAL_PACKAGE_STATUSES:
        raise DomainError("terminal_package", f"package is terminal: {status.value}")


def require_generation(actual: int, expected: int) -> None:
    if actual != expected:
        raise DomainError("stale_generation")


def control_transition_target(
    status: WorkPackageStatus, action: PackageControlAction
) -> WorkPackageStatus:
    """Validate the authoritative v1 package state machine without applying side effects."""
    allowed: dict[PackageControlAction, tuple[frozenset[WorkPackageStatus], WorkPackageStatus]] = {
        PackageControlAction.APPROVE: (
            frozenset({WorkPackageStatus.DRAFT}),
            WorkPackageStatus.APPROVED,
        ),
        PackageControlAction.START: (
            frozenset({WorkPackageStatus.APPROVED}),
            WorkPackageStatus.RUNNING,
        ),
        PackageControlAction.DISCARD: (
            frozenset({WorkPackageStatus.DRAFT, WorkPackageStatus.APPROVED}),
            WorkPackageStatus.DISCARDED,
        ),
        PackageControlAction.RETRY_ITEM: (
            frozenset({WorkPackageStatus.PAUSED}),
            WorkPackageStatus.RUNNING,
        ),
        PackageControlAction.SKIP_ITEM: (
            frozenset({WorkPackageStatus.PAUSED}),
            WorkPackageStatus.RUNNING,
        ),
        PackageControlAction.STOP_PACKAGE: (
            frozenset(
                {
                    WorkPackageStatus.DRAFT,
                    WorkPackageStatus.APPROVED,
                    WorkPackageStatus.RUNNING,
                    WorkPackageStatus.PAUSED,
                }
            ),
            WorkPackageStatus.STOPPED,
        ),
        PackageControlAction.REQUEST_REPLAN: (
            frozenset(
                {
                    WorkPackageStatus.DRAFT,
                    WorkPackageStatus.APPROVED,
                    WorkPackageStatus.RUNNING,
                    WorkPackageStatus.PAUSED,
                }
            ),
            WorkPackageStatus.DRAFT,
        ),
    }
    sources, target = allowed[action]
    if status not in sources:
        raise DomainError("invalid_transition", f"cannot {action.value} from {status.value}")
    return target
