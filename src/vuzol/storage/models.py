"""Complete MVP PostgreSQL table mappings."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from vuzol.storage.base import Base
from vuzol.storage.types import (
    ApprovalStatus,
    ControlActionStatus,
    DeliveryStatus,
    IdempotencyClass,
    InboxStatus,
    IntakeStatus,
    ProcessStatus,
    RetryClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
    enum_type,
)

JSON_OBJECT = text("'{}'::jsonb")
JSON_ARRAY = text("'[]'::jsonb")


class IdentityMixin:
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()"), onupdate=func.now()
    )


class Task(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "tasks"

    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_thread_id: Mapped[int | None] = mapped_column(BigInteger)
    project_id: Mapped[str | None] = mapped_column(String(100), index=True)
    original_text: Mapped[str] = mapped_column(Text, nullable=False)
    transcript: Mapped[str | None] = mapped_column(Text)
    voice_reference: Mapped[str | None] = mapped_column(String(500))
    task_draft: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    draft_schema_version: Mapped[str | None] = mapped_column(String(100))
    interpreter_profile: Mapped[str | None] = mapped_column(String(100))
    prompt_version: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[TaskStatus] = mapped_column(
        enum_type(TaskStatus, "task_status"), nullable=False, default=TaskStatus.RECEIVED
    )
    risk: Mapped[RiskLevel] = mapped_column(
        enum_type(RiskLevel, "risk_level"), nullable=False, default=RiskLevel.LOW
    )
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    parent_task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT")
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class Run(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "runs"

    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    workflow_type: Mapped[str] = mapped_column(String(100), nullable=False)
    workflow_version: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[RunStatus] = mapped_column(
        enum_type(RunStatus, "run_status"), nullable=False, default=RunStatus.CREATED
    )
    selected_route: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    budget_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    configuration_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    prompt_revision: Mapped[str | None] = mapped_column(String(64))
    repository_revision: Mapped[str | None] = mapped_column(String(64))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_category: Mapped[str | None] = mapped_column(String(100))
    failure_summary: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class Step(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "steps"
    __table_args__ = (
        UniqueConstraint("run_id", "ordinal", name="uq_steps_run_ordinal"),
        Index(
            "ix_steps_queue",
            "priority",
            "available_at",
            "created_at",
            postgresql_where=text("status = 'queued'"),
        ),
        Index(
            "ix_steps_lease_expiry",
            "lease_expires_at",
            postgresql_where=text("status IN ('leased', 'running')"),
        ),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    dependency_metadata: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    step_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[StepStatus] = mapped_column(
        enum_type(StepStatus, "step_status"), nullable=False, default=StepStatus.PENDING
    )
    executor_profile_id: Mapped[str | None] = mapped_column(String(100), index=True)
    required_capabilities: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    retry_class: Mapped[RetryClass] = mapped_column(
        enum_type(RetryClass, "retry_class"), nullable=False, default=RetryClass.NEVER
    )
    idempotency_class: Mapped[IdempotencyClass] = mapped_column(
        enum_type(IdempotencyClass, "idempotency_class"), nullable=False
    )
    external_idempotency_key: Mapped[str | None] = mapped_column(String(255))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_category: Mapped[str | None] = mapped_column(String(100))
    failure_summary: Mapped[str | None] = mapped_column(Text)
    unknown_effects: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class Event(IdentityMixin, Base):
    __tablename__ = "events"
    __table_args__ = (Index("ix_events_entity_time", "entity_type", "entity_id", "created_at"),)

    entity_type: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(200))
    previous_state: Mapped[str | None] = mapped_column(String(100))
    new_state: Mapped[str | None] = mapped_column(String(100))
    correlation_id: Mapped[str | None] = mapped_column(String(100), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()"), index=True
    )


class ExternalInbox(IdentityMixin, Base):
    __tablename__ = "external_inbox"
    __table_args__ = (
        UniqueConstraint(
            "source", "consumer", "external_event_id", name="uq_inbox_external_identity"
        ),
    )

    source: Mapped[str] = mapped_column(String(50), nullable=False)
    consumer: Mapped[str] = mapped_column(String(100), nullable=False)
    external_event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_payload_reference: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[InboxStatus] = mapped_column(
        enum_type(InboxStatus, "inbox_status"), nullable=False, default=InboxStatus.RECEIVED
    )
    outcome: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    linked_entity_type: Mapped[str | None] = mapped_column(String(50))
    linked_entity_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TransactionalOutbox(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "transactional_outbox"
    __table_args__ = (
        UniqueConstraint("destination", "idempotency_key", name="uq_outbox_idempotency"),
        Index(
            "ix_outbox_queue",
            "available_at",
            "created_at",
            postgresql_where=text("status IN ('pending', 'ambiguous')"),
        ),
    )

    destination: Mapped[str] = mapped_column(String(100), nullable=False)
    operation_type: Mapped[str] = mapped_column(String(100), nullable=False)
    linked_entity_type: Mapped[str] = mapped_column(String(50), nullable=False)
    linked_entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("events.id", ondelete="RESTRICT"))
    projection_revision: Mapped[int | None] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    artifact_reference: Mapped[str | None] = mapped_column(String(500))
    status: Mapped[DeliveryStatus] = mapped_column(
        enum_type(DeliveryStatus, "delivery_status"),
        nullable=False,
        default=DeliveryStatus.PENDING,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    last_error_category: Mapped[str | None] = mapped_column(String(100))
    last_error_ambiguous: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retention_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TopicMapping(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "topic_mappings"
    __table_args__ = (
        UniqueConstraint("chat_id", "message_thread_id", name="uq_topic_chat_thread"),
    )

    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_thread_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    topic_kind: Mapped[str] = mapped_column(String(50), nullable=False)
    project_id: Mapped[str | None] = mapped_column(String(100))
    accepts_new_tasks: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    default_workflow: Mapped[str] = mapped_column(String(100), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ProviderProfile(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "provider_profiles"

    stable_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    configuration_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )


class Approval(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "approvals"

    step_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("steps.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    action_envelope_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    requested_action: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_target: Mapped[str] = mapped_column(Text, nullable=False)
    human_summary: Mapped[str] = mapped_column(Text, nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[ApprovalStatus] = mapped_column(
        enum_type(ApprovalStatus, "approval_status"),
        nullable=False,
        default=ApprovalStatus.PENDING,
    )
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deciding_user_id: Mapped[int | None] = mapped_column(BigInteger)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TelegramMessageLink(IdentityMixin, Base):
    __tablename__ = "telegram_message_links"
    __table_args__ = (UniqueConstraint("chat_id", "message_id", name="uq_telegram_chat_message"),)

    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_thread_id: Mapped[int | None] = mapped_column(BigInteger)
    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tasks.id", ondelete="RESTRICT"))
    run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("runs.id", ondelete="RESTRICT"))
    step_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("steps.id", ondelete="RESTRICT"))
    approval_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("approvals.id", ondelete="RESTRICT")
    )
    message_role: Mapped[str] = mapped_column(String(50), nullable=False)
    projection_revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class TelegramIntakeMessage(IdentityMixin, Base):
    __tablename__ = "telegram_intake_messages"
    __table_args__ = (UniqueConstraint("chat_id", "message_id", name="uq_intake_chat_message"),)

    inbox_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("external_inbox.id", ondelete="RESTRICT"), nullable=False, unique=True
    )
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_thread_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT"), index=True
    )
    original_text: Mapped[str | None] = mapped_column(Text)
    attachments: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    affinity_kind: Mapped[str | None] = mapped_column(String(50))
    ambiguous_task_ids: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    status: Mapped[IntakeStatus] = mapped_column(
        enum_type(IntakeStatus, "intake_status"),
        nullable=False,
        default=IntakeStatus.RECEIVED,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class TelegramControlAction(IdentityMixin, Base):
    __tablename__ = "telegram_control_actions"

    external_action_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    action_kind: Mapped[str] = mapped_column(String(50), nullable=False)
    requested_by_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT"), index=True
    )
    step_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("steps.id", ondelete="RESTRICT"))
    approval_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("approvals.id", ondelete="RESTRICT")
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    status: Mapped[ControlActionStatus] = mapped_column(
        enum_type(ControlActionStatus, "control_action_status"),
        nullable=False,
        default=ControlActionStatus.QUEUED,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Artifact(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "artifacts"

    task_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tasks.id", ondelete="RESTRICT"))
    run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("runs.id", ondelete="RESTRICT"))
    step_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("steps.id", ondelete="RESTRICT"))
    artifact_type: Mapped[str] = mapped_column(String(100), nullable=False)
    content_uri: Mapped[str] = mapped_column(String(1000), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    media_type: Mapped[str] = mapped_column(String(200), nullable=False)
    sensitivity: Mapped[str] = mapped_column(String(50), nullable=False)
    visibility: Mapped[str] = mapped_column(String(50), nullable=False)
    retention_until: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )


class UsageRecord(IdentityMixin, Base):
    __tablename__ = "usage_records"

    provider: Mapped[str] = mapped_column(String(100), nullable=False)
    profile_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tasks.id", ondelete="RESTRICT"))
    run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("runs.id", ondelete="RESTRICT"))
    step_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("steps.id", ondelete="RESTRICT"))
    input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    output_tokens: Mapped[int | None] = mapped_column(BigInteger)
    cached_tokens: Mapped[int | None] = mapped_column(BigInteger)
    cost_units: Mapped[float | None] = mapped_column(Float)
    quota_units: Mapped[float | None] = mapped_column(Float)
    duration_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_request_id: Mapped[str | None] = mapped_column(String(255), index=True)
    outcome: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class Interpretation(IdentityMixin, Base):
    __tablename__ = "interpretations"

    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    original_input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    transcript: Mapped[str | None] = mapped_column(Text)
    task_draft: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    profile_id: Mapped[str] = mapped_column(String(100), nullable=False)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ClarificationDecision(IdentityMixin, Base):
    __tablename__ = "clarification_decisions"

    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    interpretation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("interpretations.id", ondelete="RESTRICT")
    )
    question: Mapped[str] = mapped_column(Text, nullable=False)
    answer: Mapped[str] = mapped_column(Text, nullable=False)
    deciding_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ValidationResult(IdentityMixin, Base):
    __tablename__ = "validation_results"

    step_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("steps.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    validator_type: Mapped[str] = mapped_column(String(100), nullable=False)
    command_hash: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    exit_code: Mapped[int | None] = mapped_column(Integer)
    result: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    artifact_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("artifacts.id", ondelete="RESTRICT")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class RoutingDecision(IdentityMixin, Base):
    __tablename__ = "routing_decisions"

    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    role: Mapped[str] = mapped_column(String(50), nullable=False)
    selected_profile_id: Mapped[str | None] = mapped_column(String(100))
    alternatives: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    policy_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class ProfileHealthObservation(IdentityMixin, Base):
    __tablename__ = "profile_health_observations"

    profile_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("provider_profiles.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    healthy: Mapped[bool] = mapped_column(Boolean, nullable=False)
    category: Mapped[str | None] = mapped_column(String(100))
    detail: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    unhealthy_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rate_limit_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ConfigurationRevision(IdentityMixin, Base):
    __tablename__ = "configuration_revisions"

    revision: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    kind: Mapped[str] = mapped_column(String(50), nullable=False)
    normalized_content: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    artifact_reference: Mapped[str | None] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class Worktree(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "worktrees"

    task_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tasks.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runs.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    project_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source_remote: Mapped[str | None] = mapped_column(String(1000))
    base_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    branch: Mapped[str] = mapped_column(String(255), nullable=False)
    path: Mapped[str] = mapped_column(String(1000), nullable=False, unique=True)
    owner: Mapped[str] = mapped_column(String(200), nullable=False)
    delivery_state: Mapped[WorktreeDeliveryState] = mapped_column(
        enum_type(WorktreeDeliveryState, "worktree_delivery_state"),
        nullable=False,
        default=WorktreeDeliveryState.ACTIVE,
    )
    result_commit: Mapped[str | None] = mapped_column(String(64))
    cleaned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SupervisedProcess(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "supervised_processes"

    step_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("steps.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    command_envelope_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    working_directory: Mapped[str] = mapped_column(String(1000), nullable=False)
    host_pid: Mapped[int | None] = mapped_column(BigInteger)
    container_id: Mapped[str | None] = mapped_column(String(200))
    status: Mapped[ProcessStatus] = mapped_column(
        enum_type(ProcessStatus, "process_status"),
        nullable=False,
        default=ProcessStatus.STARTING,
    )
    exit_code: Mapped[int | None] = mapped_column(Integer)
    signal_number: Mapped[int | None] = mapped_column(Integer)
    stdout_artifact_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("artifacts.id", ondelete="RESTRICT")
    )
    stderr_artifact_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("artifacts.id", ondelete="RESTRICT")
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
