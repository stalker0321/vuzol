"""Immutable, versioned contracts for the Step 09A worker trial."""

import hashlib
import json
import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionMode(StrEnum):
    SOL_SOLO = "sol_solo"
    GROK_REVIEWED = "grok_reviewed"
    GROK_GATED_SHADOW = "grok_gated_shadow"
    MULTI_GROK_REVIEWED = "multi_grok_reviewed"


class TaskClass(StrEnum):
    DOCUMENTATION_CONFIG = "documentation_config"
    PURE_MODEL_VALIDATOR = "pure_model_validator"
    FOCUSED_BUG_FIX = "focused_bug_fix"
    BOUNDED_FEATURE = "bounded_feature"
    SECURITY = "security"
    RUNTIME_LIFECYCLE = "runtime_lifecycle"
    DEPLOYMENT = "deployment"
    INFRASTRUCTURE = "infrastructure"
    UNKNOWN = "unknown"


class BoundedLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class RiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    PRIVILEGED = "privileged"


class ReviewOutcome(StrEnum):
    ACCEPTED_FIRST_PASS = "accepted_first_pass"  # noqa: S105
    ACCEPTED_AFTER_MINOR_REPAIR = "accepted_after_minor_repair"
    ACCEPTED_AFTER_MAJOR_REPAIR = "accepted_after_major_repair"
    LEAD_TAKEOVER = "lead_takeover"
    DISCARDED = "discarded"
    BLOCKED_ENVIRONMENT = "blocked_environment"
    BLOCKED_REQUIREMENTS = "blocked_requirements"


class RepairSeverity(StrEnum):
    NONE = "none"
    MINOR = "minor"
    MAJOR = "major"
    TAKEOVER = "takeover"


class DefectCategory(StrEnum):
    SCOPE_VIOLATION = "scope_violation"
    INCORRECT_IMPLEMENTATION = "incorrect_implementation"
    MISSING_EDGE_CASE = "missing_edge_case"
    WEAK_TEST = "weak_or_meaningless_test"
    TEST_BYPASSED_PRODUCTION = "test_bypassed_production_path"
    EXCEPTION_SWALLOWING = "exception_swallowing"
    FORCED_SUCCESS = "forced_success_assertion"
    INTEGRATION_MISMATCH = "integration_mismatch"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    SECURITY_REGRESSION = "security_regression"
    CONCURRENCY_LIFECYCLE = "concurrency_lifecycle_defect"
    INACCURATE_CLAIM = "inaccurate_completion_claim"
    DOCUMENTATION_ONLY = "documentation_only_defect"


class TaskClassification(FrozenModel):
    task_class: TaskClass
    complexity: BoundedLevel
    risk: RiskLevel
    testability: BoundedLevel
    blast_radius: BoundedLevel
    coupling: BoundedLevel
    novelty: BoundedLevel
    expected_file_count: int = Field(ge=0, le=100)
    affects_production: bool = False
    credentials: bool = False
    networking: bool = False
    persistence: bool = False
    concurrency: bool = False
    deployment: bool = False
    security_boundary: bool = False


class ContextEntry(FrozenModel):
    source_type: str = Field(min_length=1, max_length=100)
    reference: str = Field(min_length=1, max_length=500)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    git_blob_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{40,64}$")
    line_start: int | None = Field(default=None, ge=1)
    line_end: int | None = Field(default=None, ge=1)
    byte_count: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    repeated_from_roles: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_lines(self) -> Self:
        if (self.line_start is None) != (self.line_end is None):
            raise ValueError("context line range must provide both endpoints")
        if (
            self.line_start is not None
            and self.line_end is not None
            and self.line_end < self.line_start
        ):
            raise ValueError("context line range is reversed")
        return self

    @classmethod
    def from_content(
        cls,
        *,
        source_type: str,
        reference: str,
        content: bytes,
        git_blob_sha: str | None = None,
        line_start: int | None = None,
        line_end: int | None = None,
        repeated_from_roles: tuple[str, ...] = (),
    ) -> Self:
        return cls(
            source_type=source_type,
            reference=reference,
            content_hash=hashlib.sha256(content).hexdigest(),
            git_blob_sha=git_blob_sha,
            line_start=line_start,
            line_end=line_end,
            byte_count=len(content),
            estimated_tokens=estimate_tokens(content),
            repeated_from_roles=repeated_from_roles,
        )


class ContextManifest(FrozenModel):
    schema_version: str = "step09a-context.v1"
    role: str = Field(pattern=r"^(planner|worker|reviewer)$")
    entries: tuple[ContextEntry, ...] = ()

    @property
    def total_bytes(self) -> int:
        return sum(entry.byte_count for entry in self.entries)

    @property
    def estimated_tokens(self) -> int:
        return sum(entry.estimated_tokens for entry in self.entries)

    @property
    def repeated_bytes(self) -> int:
        return sum(entry.byte_count for entry in self.entries if entry.repeated_from_roles)


class RequiredGate(FrozenModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    command_id: str = Field(min_length=1, max_length=200)


class WorkerTaskCapsule(FrozenModel):
    schema_version: str = "step09a-task-capsule.v1"
    experiment_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    task_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    worker_profile: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    base_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    target_branch: str = Field(min_length=1, max_length=255)
    goal: str = Field(min_length=1, max_length=4_000)
    classification: TaskClassification
    predicted_mode: ExecutionMode
    actual_mode: ExecutionMode
    override_reason: str | None = Field(default=None, max_length=1_000)
    allowed_paths: tuple[str, ...] = Field(min_length=1, max_length=100)
    relevant_symbols: tuple[str, ...] = Field(default=(), max_length=100)
    acceptance_criteria: tuple[str, ...] = Field(min_length=1, max_length=100)
    forbidden_changes: tuple[str, ...] = Field(default=(), max_length=100)
    required_gates: tuple[RequiredGate, ...] = Field(min_length=1, max_length=30)
    maximum_execution_seconds: int = Field(ge=1, le=86_400)
    maximum_repair_count: int = Field(default=2, ge=0, le=2)
    context_manifest: ContextManifest
    expected_result_manifest_version: str = "step09a-worker-result.v1"
    parent_attempt: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_safety(self) -> Self:
        serialized = self.model_dump_json().lower()
        forbidden = (
            "auth.json",
            "telegram_bot_token",
            "database_url",
            "private_key",
            "/home/vodkolyan/vuzol-local",
            "/var/lib/vuzol-provider-state",
        )
        if any(marker in serialized for marker in forbidden):
            raise ValueError("task capsule contains a prohibited credential or host reference")
        if any(path.startswith("/") or ".." in path.split("/") for path in self.allowed_paths):
            raise ValueError("allowed paths must be repository-relative and contained")
        if self.predicted_mode != self.actual_mode and not self.override_reason:
            raise ValueError("execution-mode override requires a reason")
        if self.context_manifest.role != "worker":
            raise ValueError("task capsule requires a worker context manifest")
        return self


class GateResult(FrozenModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    command_id: str = Field(min_length=1, max_length=200)
    exit_code: int
    duration_ms: int = Field(ge=0)


class ReportedUsage(FrozenModel):
    input_tokens: int | None = Field(default=None, ge=0)
    cached_input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    reasoning_tokens: int | None = Field(default=None, ge=0)
    unavailable_reason: str | None = Field(default=None, max_length=500)

    @model_validator(mode="after")
    def explain_unavailable(self) -> Self:
        values = (
            self.input_tokens,
            self.cached_input_tokens,
            self.output_tokens,
            self.reasoning_tokens,
        )
        if all(value is None for value in values) and not self.unavailable_reason:
            raise ValueError("entirely unavailable usage requires an explanation")
        return self


class WorkerResultManifest(FrozenModel):
    schema_version: str = "step09a-worker-result.v1"
    experiment_id: str
    task_id: str
    worker_profile: str
    base_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    result_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    branch: str
    changed_files: tuple[str, ...]
    claimed_complete: bool
    gates: tuple[GateResult, ...]
    total_worker_duration_ms: int = Field(ge=0)
    usage: ReportedUsage
    failure_classification: str | None = Field(default=None, max_length=200)
    limitations: tuple[str, ...] = ()
    scope_exceeded: bool = False
    attempt: int = Field(default=1, ge=1, le=3)


class InvocationTelemetry(FrozenModel):
    role: str = Field(pattern=r"^(planner|worker|reviewer)$")
    profile_id: str
    model: str
    context: ContextManifest
    usage: ReportedUsage
    duration_ms: int = Field(ge=0)


class PricingRevision(FrozenModel):
    revision: str = Field(min_length=1, max_length=100)
    effective_at: datetime
    input_per_million: Decimal | None = Field(default=None, ge=0)
    output_per_million: Decimal | None = Field(default=None, ge=0)
    configured_cost_per_call: Decimal | None = Field(default=None, ge=0)


class ExperimentTelemetry(FrozenModel):
    schema_version: str = "step09a-telemetry.v1"
    experiment_id: str
    task_id: str
    task_class: TaskClass
    predicted_mode: ExecutionMode
    actual_mode: ExecutionMode
    override_reason: str | None = None
    worker_profile: str
    reviewer_profile: str | None = None
    base_commit: str
    result_commit: str | None = None
    allowed_paths: tuple[str, ...]
    actual_changed_files: tuple[str, ...] = ()
    queue_wait_ms: int = Field(ge=0)
    execution_duration_ms: int = Field(ge=0)
    gate_duration_ms: int = Field(ge=0)
    review_duration_ms: int = Field(ge=0)
    repair_duration_ms: int = Field(ge=0)
    total_wall_time_ms: int = Field(ge=0)
    invocations: tuple[InvocationTelemetry, ...]
    estimated_cost: Decimal | None = Field(default=None, ge=0)
    cost_unavailable_reason: str | None = None
    pricing_revision: PricingRevision | None = None
    worker_attempts: int = Field(ge=1, le=3)
    repair_count: int = Field(ge=0, le=2)
    repair_severity: RepairSeverity
    defect_categories: frozenset[DefectCategory] = frozenset()
    final_outcome: ReviewOutcome
    human_intervention: bool
    shadow_would_accept: bool
    shadow_decision_correct: bool
    egress_bytes: int | None = Field(default=None, ge=0)
    egress_unavailable_reason: str | None = None
    environment_variable_names: tuple[str, ...] = ()
    worker_mount_destinations: tuple[str, ...] = ()
    network_policy_id: str
    image_identity: str
    worktree_identity: str

    @model_validator(mode="after")
    def validate_honesty(self) -> Self:
        if self.estimated_cost is None and not self.cost_unavailable_reason:
            raise ValueError("unavailable cost requires an explanation")
        if self.egress_bytes is None and not self.egress_unavailable_reason:
            raise ValueError("unavailable egress requires an explanation")
        if self.repair_count > self.worker_attempts - 1:
            raise ValueError("repair count exceeds attempt lineage")
        sensitive_names = {"TELEGRAM_BOT_TOKEN", "DATABASE_URL", "VUZOL_DATABASE_DSN"}
        if sensitive_names.intersection(self.environment_variable_names):
            raise ValueError("telemetry cannot serialize sensitive environment names")
        return self

    @property
    def repeated_context_bytes(self) -> int:
        return sum(item.context.repeated_bytes for item in self.invocations)

    @property
    def total_context_bytes(self) -> int:
        return sum(item.context.total_bytes for item in self.invocations)

    @property
    def repeated_context_ratio(self) -> float:
        return (
            self.repeated_context_bytes / self.total_context_bytes
            if self.total_context_bytes
            else 0.0
        )


def estimate_tokens(content: bytes) -> int:
    """Return a labelled local estimate; provider usage remains authoritative."""
    return (len(content) + 3) // 4


def stable_json_hash(model: BaseModel) -> str:
    payload = json.dumps(model.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def new_experiment_id() -> str:
    return f"step09a-{uuid.uuid4()}"
