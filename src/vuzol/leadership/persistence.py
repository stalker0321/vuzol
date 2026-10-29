"""Additive project-lead rows. Legacy tasks never read these tables."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from vuzol.storage.base import Base
from vuzol.storage.models import IdentityMixin, TimestampMixin

JSON_OBJECT = text("'{}'::jsonb")
JSON_ARRAY = text("'[]'::jsonb")

LIVE_GOAL_LIFECYCLES = ("draft", "active", "waiting", "paused")
CONTRACT_VERSION = "project-lead.v1"
POLICY_REVISION = "project-lead-policy.v1"


class GoalDelegation(IdentityMixin, TimestampMixin, Base):
    """Owner grant. The agent principal is server-assigned and is not a user id."""

    __tablename__ = "goal_delegations"
    __table_args__ = (
        CheckConstraint(
            "agent_principal_id LIKE 'agent:%' AND agent_principal_id !~ '^agent:[0-9]+$'",
            name="goal_delegation_agent_principal",
        ),
        CheckConstraint("version >= 1", name="goal_delegation_version_positive"),
        CheckConstraint(
            "jsonb_typeof(allowed_actions) = 'array'",
            name="goal_delegation_actions_array",
        ),
    )

    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    owner_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    agent_principal_id: Mapped[str] = mapped_column(String(200), nullable=False)
    policy_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    allowed_actions: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    integration_branch: Mapped[str] = mapped_column(String(255), nullable=False)
    environments: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")


class ManagedGoal(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "managed_goals"
    __table_args__ = (
        CheckConstraint(
            "lifecycle IN ('draft','active','waiting','paused','completed','cancelled')",
            name="managed_goal_lifecycle",
        ),
        CheckConstraint("version >= 1", name="managed_goal_version_positive"),
        CheckConstraint("scope_revision >= 1", name="managed_goal_scope_revision_positive"),
        CheckConstraint("fence_epoch >= 1", name="managed_goal_fence_epoch_positive"),
        CheckConstraint("lease_epoch >= 0", name="managed_goal_lease_epoch_nonnegative"),
        CheckConstraint("repeat_count >= 0", name="managed_goal_repeat_count_nonnegative"),
        CheckConstraint("jsonb_typeof(criteria) = 'array'", name="managed_goal_criteria_array"),
        Index(
            "uq_managed_goal_live_project",
            "project_id",
            unique=True,
            postgresql_where=text("lifecycle IN ('draft','active','waiting','paused')"),
        ),
        Index(
            "uq_managed_goal_live_session",
            "discussion_session_id",
            unique=True,
            postgresql_where=text("lifecycle IN ('draft','active','waiting','paused')"),
        ),
    )

    project_id: Mapped[str] = mapped_column(String(100), nullable=False)
    owner_user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    discussion_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("project_discussion_sessions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    delegation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("goal_delegations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_chat_id: Mapped[int | None] = mapped_column(BigInteger)
    source_thread_id: Mapped[int | None] = mapped_column(BigInteger)
    original_request: Mapped[str] = mapped_column(Text, nullable=False)
    scope_text: Mapped[str] = mapped_column(Text, nullable=False)
    scope_revision: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    criteria: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    constraints: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    lifecycle: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    waiting_reason: Mapped[str | None] = mapped_column(String(80))
    wakeup_condition: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    integration_branch: Mapped[str] = mapped_column(String(255), nullable=False)
    current_package_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    proposed_plan: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    fence_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    repeat_fingerprint: Mapped[str | None] = mapped_column(String(64))
    repeat_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )


class LeadershipCommandReceipt(IdentityMixin, Base):
    __tablename__ = "leadership_command_receipts"
    __table_args__ = (
        CheckConstraint(
            "actor_kind IN ('human','agent')",
            name="leadership_receipt_actor_kind",
        ),
        CheckConstraint(
            "status IN ('applied','duplicate','stale','denied','waiting','unsupported')",
            name="leadership_receipt_status",
        ),
        CheckConstraint(
            "char_length(payload_hash) = 64",
            name="leadership_receipt_payload_hash",
        ),
        Index(
            "uq_leadership_receipt_idempotency",
            "actor_kind",
            "actor_subject",
            "idempotency_key",
            unique=True,
        ),
    )

    actor_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    actor_subject: Mapped[str] = mapped_column(String(200), nullable=False)
    ingress_source: Mapped[str] = mapped_column(String(20), nullable=False)
    command_name: Mapped[str] = mapped_column(String(64), nullable=False)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False)
    goal_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    expected_version: Mapped[int | None] = mapped_column(Integer)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    contract_version: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    result: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    reason: Mapped[str | None] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class LeadershipTurn(IdentityMixin, TimestampMixin, Base):
    """One model attempt. A lost lease cannot apply this row later."""

    __tablename__ = "leadership_turns"
    __table_args__ = (
        CheckConstraint(
            "status IN ('claimed','responded','applied','superseded','lost')",
            name="leadership_turn_status",
        ),
        CheckConstraint("lease_epoch >= 1", name="leadership_turn_lease_epoch_positive"),
        CheckConstraint("jsonb_typeof(usage) = 'object'", name="leadership_turn_usage_object"),
        Index(
            "uq_leadership_turn_wakeup",
            "goal_id",
            "wakeup_id",
            unique=True,
        ),
    )

    goal_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("managed_goals.id", ondelete="RESTRICT"),
        nullable=False,
    )
    wakeup_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    lease_epoch: Mapped[int] = mapped_column(Integer, nullable=False)
    goal_version: Mapped[int] = mapped_column(Integer, nullable=False)
    scope_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    fence_epoch: Mapped[int] = mapped_column(Integer, nullable=False)
    profile_id: Mapped[str] = mapped_column(String(100), nullable=False, default="unconfigured")
    model_assignments: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    usage: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    decision: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    owner: Mapped[str] = mapped_column(String(200), nullable=False)


class LeadershipInference(TimestampMixin, Base):
    """Immutable phase input and durable send/receipt boundary, for API and CLI alike."""

    __tablename__ = "leadership_inferences"
    __table_args__ = (
        CheckConstraint(
            "phase IN ('lead', 'goal_review', 'planner')", name="leadership_inference_phase"
        ),
        CheckConstraint(
            "state IN ('sending', 'received', 'unknown', 'not_sent')",
            name="leadership_inference_state",
        ),
        CheckConstraint("round >= 0", name="leadership_inference_round_nonnegative"),
        Index("uq_leadership_inference_phase", "turn_id", "phase", "round", unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    turn_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("leadership_turns.id", ondelete="RESTRICT"), nullable=False
    )
    phase: Mapped[str] = mapped_column(String(20), nullable=False)
    round: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    profile_id: Mapped[str] = mapped_column(String(100), nullable=False)
    profile_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String(20), nullable=False)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    tool_audit: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=JSON_ARRAY
    )
    reservation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("provider_budget_reservations.id", ondelete="RESTRICT"), nullable=False
    )


def has_owner_mandate(goal: ManagedGoal) -> bool:
    """Legacy owner-enabled goals retain their grant; text drafts require plan approval."""
    return not goal.constraints.get("mandate_required") or (
        (goal.constraints.get("owner_mandate") or {}).get("scope_revision") == goal.scope_revision
    )


async def lead_accepts_results(session: AsyncSession, goal: ManagedGoal) -> bool:
    """The owner delegated item acceptance: the lead accepts each item, the owner the goal.

    Live pilot 2026-10-25: every item's card asked the owner to accept it, and the
    lead's own acceptance raced the owner's button.
    """
    from datetime import UTC

    from vuzol.leadership.rules import delegation_is_expired

    if goal.lifecycle in {"cancelled", "completed", "paused"} or not has_owner_mandate(goal):
        return False
    delegation = await session.get(GoalDelegation, goal.delegation_id)
    return (
        delegation is not None
        and delegation.revoked_at is None
        and not delegation_is_expired(delegation.expires_at, now=datetime.now(UTC))
        and bool({"accept_result", "apply_integration"} & set(delegation.allowed_actions))
    )


def current_request(goal: ManagedGoal) -> str:
    """What the goal asks for now: after an owner amendment the amended scope is the goal
    and the first wording is history (live pilot 2026-10-24: lead, reviewer and cards
    kept working on the superseded request)."""
    return goal.scope_text if goal.scope_revision > 1 else goal.original_request


def superseded_request(goal: ManagedGoal) -> str | None:
    return goal.original_request if goal.scope_revision > 1 else None


class ManagedGoalEvidence(IdentityMixin, Base):
    """Checker verdict for one criterion at one scope revision and result commit.

    The criterion id is the address. ``satisfied`` is the checker's verdict
    about the description, never a plan item's self-declared id and never an
    owner waiver.
    """

    __tablename__ = "managed_goal_evidence"
    __table_args__ = (
        Index(
            "uq_managed_goal_evidence_binding",
            "goal_id",
            "criterion_id",
            "scope_revision",
            "result_commit",
            unique=True,
        ),
    )

    goal_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("managed_goals.id", ondelete="RESTRICT"),
        nullable=False,
    )
    criterion_id: Mapped[str] = mapped_column(String(80), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    scope_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    result_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    observation: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    satisfied: Mapped[bool] = mapped_column(Boolean, nullable=False)
    checker: Mapped[str] = mapped_column(String(40), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class LeadJob(IdentityMixin, Base):
    """Read-only background work, independent of the single live work package."""

    __tablename__ = "lead_jobs"
    __table_args__ = (
        CheckConstraint("kind = 'research'", name="lead_job_kind"),
        CheckConstraint(
            "state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="lead_job_state",
        ),
        CheckConstraint(
            "jsonb_typeof(input) = 'object' AND input ? 'question' "
            "AND jsonb_typeof(input->'question') = 'string' "
            "AND char_length(btrim(input->>'question')) BETWEEN 1 AND 2000",
            name="lead_job_input",
        ),
        CheckConstraint("scope_revision >= 1", name="lead_job_scope_revision"),
        CheckConstraint("lease_generation >= 0", name="lead_job_lease_generation"),
        CheckConstraint("octet_length(result_text) <= 16384", name="lead_job_result_bytes"),
        CheckConstraint("jsonb_typeof(usage) = 'object'", name="lead_job_usage"),
        CheckConstraint(
            "(state = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (state <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="lead_job_lease",
        ),
        CheckConstraint(
            "(state IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL)",
            name="lead_job_finished",
        ),
        Index(
            "ix_lead_jobs_queued",
            "created_at",
            "id",
            postgresql_where=text("state = 'queued'"),
        ),
        Index(
            "ix_lead_jobs_running_expiry",
            "lease_expires_at",
            postgresql_where=text("state = 'running'"),
        ),
    )

    goal_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("managed_goals.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    input: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    pinned_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    scope_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(
        String(20), nullable=False, default="queued", server_default="queued"
    )
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_generation: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    result_text: Mapped[str | None] = mapped_column(Text)
    failure_reason: Mapped[str | None] = mapped_column(String(512))
    usage: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_OBJECT
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
