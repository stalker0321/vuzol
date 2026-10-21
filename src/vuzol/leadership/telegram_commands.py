"""Telegram text commands for an existing project-lead goal."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.settings import Settings
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.persistence import CONTRACT_VERSION, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import IngressResult, IngressStatus, MessageUpdate
from vuzol.telegram.projections import enqueue_chat_notice
from vuzol.workflows.application import Principal

# Re-export the contract constant under the name callers already use.
_CONTRACT = CONTRACT_VERSION


async def apply_telegram_lead(
    *,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    update: MessageUpdate,
    project_id: str,
    default_branch: str,
    kind: str,
    argument: str,
) -> IngressResult:
    """Apply one owner command. The notice is a second transaction."""

    session_id = await _discussion_session(
        session_factory,
        project_id=project_id,
        chat_id=update.chat_id,
        message_thread_id=update.message_thread_id,
    )
    goal_id, version = await _live_goal(session_factory, project_id=project_id)
    command = _command(
        kind=kind,
        argument=argument,
        project_id=project_id,
        session_id=session_id,
        goal_id=goal_id,
        version=version,
        update=update,
        default_branch=default_branch,
        prefix=settings.project_lead.integration_branch_prefix,
    )
    if command is None:
        return IngressResult(status=IngressStatus.REJECTED, reason="lead_command_invalid")
    service = LeadershipCommandService(
        session_factory,
        enabled=settings.project_lead.enabled,
        branch_prefix=settings.project_lead.integration_branch_prefix,
        repeat_limit=settings.project_lead.repeat_limit,
        default_branches={project_id: default_branch},
    )
    result = await service.apply_human(
        Principal(user_id=update.user_id, ingress_source="telegram"),
        command,
    )
    notice = await _notice(session_factory, project_id, result.status, result.reason)
    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        await enqueue_chat_notice(
            uow.session,
            chat_id=update.chat_id,
            thread_id=update.message_thread_id,
            key=f"lead:notice:{update.update_id}",
            text=notice,
        )
    return IngressResult(status=IngressStatus.HANDLED, reason=result.status)


def _command(
    *,
    kind: str,
    argument: str,
    project_id: str,
    session_id: uuid.UUID,
    goal_id: uuid.UUID | None,
    version: int | None,
    update: MessageUpdate,
    default_branch: str,
    prefix: str,
) -> LeadCommand | None:
    key = f"telegram:{update.bot_id}:{update.update_id}:{kind}"
    if kind == "enable":
        limits: dict[str, int] = {}
        while argument.startswith("--"):
            parts = argument.split(maxsplit=2)
            fields = {"--max-packages": "max_packages", "--max-items": "max_items_per_package"}
            if len(parts) != 3 or parts[0] not in fields or not parts[1].isdigit():
                return None
            limits[fields[parts[0]]] = int(parts[1])
            argument = parts[2]
        branch = f"{prefix}{project_id}/{session_id.hex[:12]}"
        return LeadCommand(
            name="enable_managed_goal",
            project_id=project_id,
            idempotency_key=key,
            contract_version=_CONTRACT,
            payload={
                "session_id": str(session_id),
                "integration_branch": branch,
                "request": argument,
                "scope": argument,
                "criteria": [{"id": "requested-outcome", "description": argument}],
                "project_default_branch": default_branch,
                "work_limits": limits,
            },
        )
    names = {
        "pause": "pause_goal",
        "resume": "resume_goal",
        "cancel": "cancel_goal",
        "revoke": "revoke_delegation",
        "status": "get_goal",
        "answer": "answer_permission",
        "limits": "set_work_limits",
    }
    name = names.get(kind)
    answer_payload: dict[str, Any] = {}
    if kind == "limits":
        try:
            packages, items = argument.split()
            answer_payload = {
                "work_limits": {
                    "max_packages": None if packages == "unlimited" else int(packages),
                    "max_items_per_package": None if items == "unlimited" else int(items),
                }
            }
        except ValueError:
            return None
    if kind == "answer":
        try:
            target, question_version, answer = argument.split(maxsplit=2)
            goal_id = uuid.UUID(target)
            answer_payload = {"answer": answer, "question_version": int(question_version)}
        except (ValueError, TypeError):
            return None
    if name is None or goal_id is None:
        if name is None:
            return None
        return LeadCommand(
            name=name,
            project_id=project_id,
            goal_id=None,
            idempotency_key=key,
            contract_version=_CONTRACT,
            payload=answer_payload,
        )
    return LeadCommand(
        name=name,
        project_id=project_id,
        goal_id=goal_id,
        expected_version=None if name == "get_goal" else version,
        idempotency_key=key,
        contract_version=_CONTRACT,
        payload=answer_payload,
    )


async def _notice(
    session_factory: async_sessionmaker[AsyncSession],
    project_id: str,
    status: str,
    reason: str | None,
) -> str:
    from vuzol.leadership.card import render_goal_card
    from vuzol.leadership.persistence import GoalDelegation
    from vuzol.leadership.service import goal_card_view

    line = f"Команда: {status}" + (f" ({reason})" if reason else "")
    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        goal = await uow.session.scalar(
            select(ManagedGoal)
            .where(ManagedGoal.project_id == project_id)
            .order_by(ManagedGoal.created_at.desc())
            .limit(1)
        )
        if goal is None:
            return f"Руководитель\n{line}"
        delegation = await uow.session.get(GoalDelegation, goal.delegation_id)
        if delegation is None:
            return f"Руководитель\n{line}"
        view = await goal_card_view(uow.session, goal, delegation)
    return f"{render_goal_card(view)}\n{line}"


async def _discussion_session(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    project_id: str,
    chat_id: int,
    message_thread_id: int,
) -> uuid.UUID:
    async with UnitOfWork(session_factory) as uow:
        existing = await uow.discussions.active_session_id(
            chat_id=chat_id, message_thread_id=message_thread_id
        )
        if existing is not None:
            return existing
        return await uow.discussions.create_session(
            project_id=project_id,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
        )


async def _live_goal(
    session_factory: async_sessionmaker[AsyncSession], *, project_id: str
) -> tuple[uuid.UUID | None, int | None]:
    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        goal = await uow.session.scalar(
            select(ManagedGoal)
            .where(ManagedGoal.project_id == project_id)
            .order_by(ManagedGoal.created_at.desc())
            .limit(1)
        )
        if goal is None:
            return None, None
        return goal.id, goal.version
