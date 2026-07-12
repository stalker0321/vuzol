"""Stable database vocabularies shared by mappings and repositories."""

from enum import StrEnum

from sqlalchemy import Enum as SqlEnum


class TaskStatus(StrEnum):
    RECEIVED = "received"
    INTERPRETED = "interpreted"
    CONTEXT_PREPARED = "context_prepared"
    PLANNED = "planned"
    WAITING_APPROVAL = "waiting_approval"
    EXECUTING = "executing"
    VALIDATING = "validating"
    REVIEWING = "reviewing"
    AWAITING_USER = "awaiting_user"
    PAUSED = "paused"
    RETRYING = "retrying"
    QUOTA_EXHAUSTED = "quota_exhausted"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ROLLED_BACK = "rolled_back"
    COMPLETED = "completed"


class RunStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    AWAITING_USER = "awaiting_user"
    PAUSED = "paused"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class StepStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    AWAITING_USER = "awaiting_user"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class RiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    PRIVILEGED = "privileged"


class RetryClass(StrEnum):
    NEVER = "never"
    TRANSIENT = "transient"
    POLICY = "policy"


class IdempotencyClass(StrEnum):
    READ_ONLY = "read_only"
    IDEMPOTENT = "idempotent"
    ISOLATED_RETRYABLE = "isolated_retryable"
    NON_IDEMPOTENT = "non_idempotent"
    UNKNOWN_EFFECTS_POSSIBLE = "unknown_effects_possible"


class QueueClass(StrEnum):
    CONTROL = "control"
    LIGHT = "light"
    HEAVY = "heavy"
    PRIVILEGED = "privileged"


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CONSUMED = "consumed"


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    LEASED = "leased"
    DELIVERED = "delivered"
    AMBIGUOUS = "ambiguous"
    DEAD_LETTER = "dead_letter"


class InboxStatus(StrEnum):
    RECEIVED = "received"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FAILED = "failed"


class WorktreeDeliveryState(StrEnum):
    ACTIVE = "active"
    WORKTREE_RETAINED = "worktree_retained"
    PATCH_DELIVERED = "patch_delivered"
    APPLIED = "applied"
    MERGED = "merged"
    PUSHED = "pushed"
    CLEANED = "cleaned"


class ProcessStatus(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    TERMINATING = "terminating"
    EXITED = "exited"
    UNKNOWN = "unknown"


class IntakeStatus(StrEnum):
    RECEIVED = "received"
    AWAITING_INTERPRETATION = "awaiting_interpretation"
    NEEDS_CLARIFICATION = "needs_clarification"
    REJECTED = "rejected"


class ControlActionStatus(StrEnum):
    QUEUED = "queued"
    PROCESSED = "processed"
    REJECTED = "rejected"


def enum_type(enum: type[StrEnum], name: str, *, length: int | None = None) -> SqlEnum:
    if length is not None:
        return SqlEnum(
            enum,
            name=name,
            native_enum=False,
            create_constraint=True,
            length=length,
            values_callable=lambda members: [member.value for member in members],
            validate_strings=True,
        )
    return SqlEnum(
        enum,
        name=name,
        native_enum=False,
        create_constraint=True,
        values_callable=lambda members: [member.value for member in members],
        validate_strings=True,
    )
