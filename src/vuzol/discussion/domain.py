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
    FINISH_PACKAGE = "finish_package"
    RESTART_PACKAGE = "restart_package"
    REQUEST_REPLAN = "request_replan"


class DomainError(RuntimeError):
    """Stable, fail-closed domain rejection suitable for application mapping."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


class ComponentKind(StrEnum):
    STATIC_SITE = "static_site"
    WEB_SERVICE = "web_service"
    ANDROID_APP = "android_app"
    CLI = "cli"
    LIBRARY = "library"
    BOT = "bot"
    MCP_SERVER = "mcp_server"
    WORKER = "worker"
    DATABASE = "database"
    OTHER = "other"


class CapabilityProvisioning(StrEnum):
    AUTOMATIC = "automatic"
    APPROVAL_REQUIRED = "approval_required"
    EXTERNAL_SETUP = "external_setup"


@dataclass(frozen=True, slots=True)
class EnvironmentComponentDraft:
    key: str
    label: str
    kind: ComponentKind
    technology: str
    version: str | None = None
    run_command: tuple[str, ...] = ()
    port: int | None = None
    healthcheck_path: str | None = None
    artifact_patterns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if _SLUG.fullmatch(self.key) is None or len(self.key) > 64:
            raise DomainError("invalid_environment", "component key must be a bounded slug")
        if not self.label.strip() or len(self.label) > 100:
            raise DomainError("invalid_environment", "component label must contain 1..100 chars")
        if not self.technology.strip() or len(self.technology) > 100:
            raise DomainError("invalid_environment", "component technology is required")
        if self.port is not None and not 1 <= self.port <= 65535:
            raise DomainError("invalid_environment", "component port is invalid")
        if self.healthcheck_path is not None and not self.healthcheck_path.startswith("/"):
            raise DomainError("invalid_environment", "healthcheck path must be absolute")
        if self.kind is ComponentKind.WEB_SERVICE and not self.run_command:
            raise DomainError("invalid_environment", "web service requires a run command")
        if self.kind is ComponentKind.WEB_SERVICE and self.port is None:
            raise DomainError("invalid_environment", "web service requires a port")


@dataclass(frozen=True, slots=True)
class CapabilityRequirementDraft:
    key: str
    label: str
    provisioning: CapabilityProvisioning = CapabilityProvisioning.AUTOMATIC
    reason: str = ""

    def __post_init__(self) -> None:
        if _SLUG.fullmatch(self.key) is None or len(self.key) > 64:
            raise DomainError("invalid_environment", "capability key must be a bounded slug")
        if not self.label.strip() or len(self.label) > 100:
            raise DomainError("invalid_environment", "capability label must contain 1..100 chars")
        if len(self.reason) > 500:
            raise DomainError("invalid_environment", "capability reason is too long")


@dataclass(frozen=True, slots=True)
class EnvironmentDeltaDraft:
    upsert_components: tuple[EnvironmentComponentDraft, ...] = ()
    remove_components: tuple[str, ...] = ()
    required_capabilities: tuple[CapabilityRequirementDraft, ...] = ()

    def __post_init__(self) -> None:
        keys = [component.key for component in self.upsert_components]
        if len(keys) != len(set(keys)):
            raise DomainError("invalid_environment", "component keys must be unique")
        if len(self.remove_components) != len(set(self.remove_components)) or any(
            _SLUG.fullmatch(key) is None for key in self.remove_components
        ):
            raise DomainError("invalid_environment", "removed component keys must be unique slugs")
        if set(keys) & set(self.remove_components):
            raise DomainError("invalid_environment", "a component cannot be added and removed")
        capabilities = [capability.key for capability in self.required_capabilities]
        if len(capabilities) != len(set(capabilities)):
            raise DomainError("invalid_environment", "capability keys must be unique")

    @property
    def is_empty(self) -> bool:
        return not (self.upsert_components or self.remove_components or self.required_capabilities)


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
    environment_delta: EnvironmentDeltaDraft = EnvironmentDeltaDraft()

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
    body: dict[str, Any] = {
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
    if not plan.environment_delta.is_empty:
        body["environment_delta"] = canonical_environment_delta(plan.environment_delta)
    return body


def canonical_environment_delta(delta: EnvironmentDeltaDraft) -> dict[str, Any]:
    return {
        "upsert_components": [
            {
                "key": component.key,
                "label": component.label.strip(),
                "kind": component.kind.value,
                "technology": component.technology.strip(),
                "version": component.version,
                "run_command": list(component.run_command),
                "port": component.port,
                "healthcheck_path": component.healthcheck_path,
                "artifact_patterns": list(component.artifact_patterns),
            }
            for component in delta.upsert_components
        ],
        "remove_components": list(delta.remove_components),
        "required_capabilities": [
            {
                "key": capability.key,
                "label": capability.label.strip(),
                "provisioning": capability.provisioning.value,
                "reason": capability.reason.strip(),
            }
            for capability in delta.required_capabilities
        ],
    }


def canonical_plan_hash(body: dict[str, Any]) -> str:
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def semantic_plan_hash(plan: PlanDraft) -> str:
    """Fingerprint user-visible plan meaning without package-local identities."""

    body: dict[str, Any] = {
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
    if not plan.environment_delta.is_empty:
        body["environment_delta"] = canonical_environment_delta(plan.environment_delta)
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
    if isinstance(body.get("environment_delta"), dict):
        projected["environment_delta"] = body["environment_delta"]
    return canonical_plan_hash(projected)


def plan_outline_hash(plan: PlanDraft) -> str:
    """Fingerprint the stable user-visible outline, ignoring model paraphrases."""

    body: dict[str, Any] = {
        "title": _normalized_outline_text(plan.title),
        "items": [_normalized_outline_text(item.summary) for item in plan.items],
    }
    if not plan.environment_delta.is_empty:
        body["environment_delta"] = canonical_environment_delta(plan.environment_delta)
    return canonical_plan_hash(body)


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
    projected: dict[str, Any] = {
        "title": _normalized_outline_text(str(body.get("title", ""))),
        "items": summaries,
    }
    if isinstance(body.get("environment_delta"), dict):
        projected["environment_delta"] = body["environment_delta"]
    return canonical_plan_hash(projected)


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
        PackageControlAction.FINISH_PACKAGE: (
            frozenset(
                {
                    WorkPackageStatus.APPROVED,
                    WorkPackageStatus.RUNNING,
                    WorkPackageStatus.PAUSED,
                    WorkPackageStatus.STOPPED,
                }
            ),
            WorkPackageStatus.DISCARDED,
        ),
        PackageControlAction.RESTART_PACKAGE: (
            frozenset({WorkPackageStatus.STOPPED}),
            WorkPackageStatus.RUNNING,
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
            WorkPackageStatus.PAUSED,
        ),
    }
    sources, target = allowed[action]
    if status not in sources:
        raise DomainError("invalid_transition", f"cannot {action.value} from {status.value}")
    return target
