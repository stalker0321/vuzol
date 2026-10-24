"""Atomic text admission shared by Telegram and external assistants."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RuntimeConfiguration
from vuzol.discussion.memory_service import DiscussionMemoryService
from vuzol.leadership.actor import server_agent_principal
from vuzol.leadership.commands import DRAFT_ACTIONS, LeadCommand, payload_hash
from vuzol.leadership.fence import lock_project_admission
from vuzol.leadership.persistence import (
    CONTRACT_VERSION,
    LIVE_GOAL_LIFECYCLES,
    POLICY_REVISION,
    GoalDelegation,
    LeadershipCommandReceipt,
    ManagedGoal,
)
from vuzol.leadership.provenance import RequestOrigin
from vuzol.storage.models import (
    Event,
    ProjectDiscussionSession,
    TelegramIntakeMessage,
    TransactionalOutbox,
)
from vuzol.storage.types import (
    ConversationTurnRole,
    ConversationTurnSource,
    DiscussionSessionStatus,
    IntakeStatus,
    InteractionMode,
)
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.application import Principal


@dataclass(frozen=True)
class EventReceipt:
    status: str
    event_id: uuid.UUID | None
    goal_id: uuid.UUID | None
    receipt_id: uuid.UUID | None
    reason: str | None = None


async def live_or_draft(
    session: AsyncSession,
    *,
    project_id: str,
    owner_user_id: int,
    discussion: ProjectDiscussionSession,
    text: str,
    scope: str,
    criteria: list[dict[str, Any]],
    constraints: dict[str, Any],
    branch_prefix: str,
) -> ManagedGoal:
    """Caller holds project admission before locking goal or discussion rows."""
    goal = await session.scalar(
        select(ManagedGoal)
        .where(
            ManagedGoal.project_id == project_id,
            ManagedGoal.lifecycle.in_(LIVE_GOAL_LIFECYCLES),
        )
        .with_for_update()
    )
    if goal is not None:
        if goal.owner_user_id != owner_user_id:
            raise PermissionError("goal_not_available")
        return goal
    goal_id = uuid.uuid4()
    branch = f"{branch_prefix}{project_id}/{goal_id.hex}"
    delegation = GoalDelegation(
        project_id=project_id,
        owner_user_id=owner_user_id,
        agent_principal_id=server_agent_principal(project_id),
        policy_revision=POLICY_REVISION,
        allowed_actions=sorted(DRAFT_ACTIONS),
        integration_branch=branch,
        environments=["isolated"],
    )
    session.add(delegation)
    await session.flush()
    goal = ManagedGoal(
        id=goal_id,
        project_id=project_id,
        owner_user_id=owner_user_id,
        discussion_session_id=discussion.id,
        delegation_id=delegation.id,
        source_chat_id=discussion.chat_id,
        source_thread_id=discussion.message_thread_id,
        original_request=text,
        scope_text=scope,
        criteria=criteria or [{"id": "requested-outcome", "description": scope}],
        constraints={**constraints, "mandate_required": True},
        lifecycle="draft",
        integration_branch=branch,
    )
    session.add(goal)
    await session.flush()
    return goal


class ProjectEventService:
    def __init__(self, runtime: RuntimeConfiguration) -> None:
        self.runtime = runtime

    async def submit_event(
        self,
        uow: UnitOfWork,
        principal: Principal,
        *,
        project_id: str,
        session_id: uuid.UUID,
        idempotency_key: str,
        text: str,
        scope: str | None = None,
        criteria: list[dict[str, Any]] | None = None,
        work_limits: dict[str, Any] | None = None,
        origin: RequestOrigin,
        intake: TelegramIntakeMessage | None = None,
        reply_to_message_id: int | None = None,
    ) -> EventReceipt:
        """Record a message, never interpret it or grant execution authority."""
        assert uow.session is not None
        session = uow.session
        settings = self.runtime.settings
        if not settings.project_lead.enabled:
            return EventReceipt("denied", None, None, None, "feature_disabled")
        project = self.runtime.registries.projects.get(project_id)
        if principal.user_id not in settings.allowed_user_ids or not project.enabled:
            raise PermissionError("project_not_available")
        if not text.strip():
            raise ValueError("event_invalid")
        await lock_project_admission(session, project_id)
        await session.execute(
            select(
                func.pg_advisory_xact_lock(
                    func.hashtextextended(
                        f"lead-command:human:{principal.user_id}:{idempotency_key}", 0
                    )
                )
            )
        )
        discussion = await session.get(ProjectDiscussionSession, session_id)
        if (
            discussion is None
            or discussion.project_id != project_id
            or discussion.status is not DiscussionSessionStatus.ACTIVE
            or discussion.chat_id not in settings.allowed_chat_ids
        ):
            raise PermissionError("project_session_not_available")
        if intake is not None and (
            intake.user_id != principal.user_id
            or (intake.chat_id, intake.message_thread_id)
            != (discussion.chat_id, discussion.message_thread_id)
        ):
            raise PermissionError("event_outside_scope")
        from vuzol.leadership.limits import parse_work_limits

        limits = parse_work_limits(work_limits or {}).model_dump()
        body = {
            "session_id": str(session_id),
            "text": text,
            "scope": scope or text,
            "criteria": criteria or [],
            "work_limits": limits,
            "reply_to_message_id": reply_to_message_id,
            "attachments": intake.attachments if intake else [],
        }
        digest = payload_hash(
            LeadCommand(
                name="submit_event",
                project_id=project_id,
                idempotency_key=idempotency_key,
                payload=body,
            )
        )
        prior = await session.scalar(
            select(LeadershipCommandReceipt).where(
                LeadershipCommandReceipt.actor_kind == "human",
                LeadershipCommandReceipt.actor_subject == str(principal.user_id),
                LeadershipCommandReceipt.idempotency_key == idempotency_key,
            )
        )
        if prior is not None:
            if prior.payload_hash != digest or prior.project_id != project_id:
                return EventReceipt("denied", None, None, prior.id, "idempotency_conflict")
            return EventReceipt(
                "duplicate",
                uuid.UUID(prior.result["event_id"]),
                prior.goal_id,
                prior.id,
                "duplicate",
            )
        provenance = {**origin.model_dump(mode="json"), "authorized_user_id": principal.user_id}
        goal = await live_or_draft(
            session,
            project_id=project_id,
            owner_user_id=principal.user_id,
            discussion=discussion,
            text=text,
            scope=scope or text,
            criteria=criteria or [],
            constraints={
                "work_limits": limits,
                "project_default_branch": project.default_branch,
                "request_origin": provenance,
            },
            branch_prefix=settings.project_lead.integration_branch_prefix,
        )
        # Use one database clock across ingress nodes, after admission serialized them.
        received_at = await session.scalar(select(func.clock_timestamp()))
        assert received_at is not None
        event_id = await write_owner_event(
            uow,
            goal=goal,
            session_id=session_id,
            key=idempotency_key,
            text=text,
            intake=intake,
            reply_to_message_id=reply_to_message_id,
            extra={
                # intake.created_at is transaction-start time, before any lock wait.
                "received_at": received_at.astimezone(UTC).isoformat(timespec="microseconds"),
                "request_origin": provenance,
                "request_scope": scope or text,
                "request_criteria": criteria or [],
                "request_work_limits": limits,
            },
        )
        receipt = LeadershipCommandReceipt(
            actor_kind="human",
            actor_subject=str(principal.user_id),
            ingress_source=principal.ingress_source,
            command_name="submit_event",
            project_id=project_id,
            goal_id=goal.id,
            expected_version=None,
            idempotency_key=idempotency_key,
            payload_hash=digest,
            payload={**body, "request_origin": provenance},
            contract_version=CONTRACT_VERSION,
            status="applied",
            result={"event_id": str(event_id)},
        )
        session.add(receipt)
        session.add(
            Event(
                entity_type="managed_goal",
                entity_id=goal.id,
                event_type="managed_goal.request_received",
                actor_type="owner",
                actor_id=str(principal.user_id),
                payload={
                    "event_id": str(event_id),
                    "origin": provenance,
                    "request": text,
                    "scope": scope or text,
                    "criteria": criteria or [],
                },
            )
        )
        if origin.channel not in {"telegram", "cli"}:
            from vuzol.leadership.text import owner_text
            from vuzol.telegram.projections import enqueue_chat_notice

            context = f"\n\n{scope}" if scope and scope != text else ""
            heading = (
                "Задача из обсуждения:"
                if origin.channel == "think"
                else f"Сообщение от {origin.display_name}:"
            )
            await enqueue_chat_notice(
                session,
                chat_id=discussion.chat_id,
                thread_id=discussion.message_thread_id,
                key=f"lead:incoming:{event_id}",
                text=owner_text(f"{heading}\n{text}{context}"),
            )
        await session.flush()
        return EventReceipt("accepted", event_id, goal.id, receipt.id)


async def write_owner_event(
    uow: UnitOfWork,
    *,
    goal: ManagedGoal,
    session_id: uuid.UUID,
    key: str,
    text: str,
    intake: TelegramIntakeMessage | None = None,
    reply_to_message_id: int | None = None,
    extra: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Write the conversation and outbox in the caller's admission transaction."""
    assert uow.session is not None
    source_turn_id, _ = await DiscussionMemoryService(uow).append_turn(
        session_id=session_id,
        role=ConversationTurnRole.USER,
        source=ConversationTurnSource.TELEGRAM_USER if intake else ConversationTurnSource.CONTROL,
        content=text,
        classifier_mode=InteractionMode.DISCUSSION,
        intake_message_id=intake.id if intake else None,
    )
    reply_context: dict[str, Any] | None = None
    if reply_to_message_id is not None and intake is not None:
        from vuzol.storage.models import PlanRevision, TelegramMessageLink

        link = await uow.session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.chat_id == intake.chat_id,
                TelegramMessageLink.message_thread_id == intake.message_thread_id,
                TelegramMessageLink.message_id == reply_to_message_id,
            )
        )
        reply_context = {"message_id": reply_to_message_id}
        if link is not None:
            reply_context["role"] = link.message_role
            if link.plan_revision_id:
                revision = await uow.session.get(PlanRevision, link.plan_revision_id)
                if revision is not None:
                    reply_context.update(
                        {
                            "package_id": str(link.work_package_id),
                            "plan_revision_id": str(revision.id),
                            "plan": revision.immutable_body,
                        }
                    )
    event_id = uuid.uuid4()
    received = intake.created_at if intake else datetime.now(UTC)
    uow.session.add(
        TransactionalOutbox(
            id=event_id,
            destination="project_lead",
            operation_type="lead_wakeup",
            linked_entity_type="managed_goal",
            linked_entity_id=goal.id,
            idempotency_key=key,
            payload={
                "goal_id": str(goal.id),
                "wakeup_kind": "owner_message",
                "source_event_id": str(event_id),
                "intake_id": str(intake.id) if intake else str(event_id),
                "received_at": received.astimezone(UTC).isoformat(timespec="microseconds"),
                "source_turn_id": str(source_turn_id),
                "text": text,
                **({"reply_context": reply_context} if reply_context else {}),
                "attachments": intake.attachments if intake else [],
                "owner_user_id": goal.owner_user_id,
                "delegation_id": str(goal.delegation_id),
                "goal_version": goal.version,
                "scope_revision": goal.scope_revision,
                "session_id": str(session_id),
                "chat_id": intake.chat_id if intake else goal.source_chat_id,
                "message_thread_id": intake.message_thread_id if intake else goal.source_thread_id,
                "message_id": intake.message_id if intake else None,
                **(extra or {}),
            },
        )
    )
    if intake is not None:
        intake.status = IntakeStatus.COMPLETED
    await uow.session.flush()
    return event_id
