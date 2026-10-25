"""Telegram text commands for an existing project-lead goal."""
# ruff: noqa: RUF001

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.settings import Settings
from vuzol.leadership.commands import CommandResult, LeadCommand
from vuzol.leadership.persistence import CONTRACT_VERSION, LeadershipCommandReceipt, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.storage.models import TransactionalOutbox
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.domain import ControlUpdate, IngressResult, IngressStatus, MessageUpdate
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
    """Apply an owner command; mutations publish their card with the command receipt."""

    await _discussion_session(
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
        goal_id=goal_id,
        version=version,
        update=update,
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
    # A mutating command already enqueues its state card in the receipt transaction.
    # Do not send a second copy. Replays consult the immutable receipt, not today's
    # version; failed commands still need their existing separate error notice.
    original_status = result.status
    notice_result = result
    if result.status == "duplicate" and result.receipt_id is not None:
        async with session_factory() as session:
            receipt = await session.get(LeadershipCommandReceipt, result.receipt_id)
            if receipt is not None:
                original_status = receipt.status
                notice_result = replace(result, status=receipt.status, reason=receipt.reason)
    if kind not in {"status", "goal", "details", "debug"} and original_status in {
        "applied",
        "waiting",
    }:
        async with session_factory() as session:
            state_notice = await session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key
                    == f"lead:state:{result.goal_id}:{result.version}"
                )
            )
            if (
                state_notice is not None
                and state_notice.payload.get("chat_id") == update.chat_id
                and state_notice.payload.get("message_thread_id") == update.message_thread_id
            ):
                return IngressResult(status=IngressStatus.HANDLED, reason=result.status)
    notice = await _notice(
        session_factory, project_id, notice_result, kind=kind, user_id=update.user_id
    )
    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        status_goal = await uow.session.get(ManagedGoal, result.goal_id) if result.goal_id else None
        key = f"lead:{'debug' if kind == 'debug' else 'notice'}:{update.bot_id}:{update.update_id}"
        if kind == "status" and status_goal and original_status == "applied":
            key = f"lead:state:{status_goal.id}:{result.version}"
            if (status_goal.source_chat_id, status_goal.source_thread_id) != (
                update.chat_id,
                update.message_thread_id,
            ):
                key += f":{update.chat_id}:{update.message_thread_id}"
        await enqueue_chat_notice(
            uow.session,
            chat_id=update.chat_id,
            thread_id=update.message_thread_id,
            key=key,
            text=notice,
            markdown=True,
            projection_role=(
                f"ls:{result.goal_id.hex}"
                if kind == "status" and result.goal_id and original_status == "applied"
                else None
            ),
            revision=result.version or 0,
            callback_buttons=goal_status_buttons(status_goal)
            if kind == "status" and status_goal
            else (),
        )
    return IngressResult(status=IngressStatus.HANDLED, reason=result.status)


def normalize_lead_message(argument: str) -> tuple[str, dict[str, int]]:
    """Keep explicit legacy limits as message metadata, without enabling a goal."""
    limits: dict[str, int] = {}
    fields = {"--max-packages": "max_packages", "--max-items": "max_items_per_package"}
    while argument.startswith("--"):
        parts = argument.split(maxsplit=2)
        if len(parts) != 3 or parts[0] not in fields or not parts[1].isdigit():
            raise ValueError("lead_command_invalid")
        limits[fields[parts[0]]] = int(parts[1])
        argument = parts[2]
    return argument, limits


def _command(
    *,
    kind: str,
    argument: str,
    project_id: str,
    goal_id: uuid.UUID | None,
    version: int | None,
    update: MessageUpdate,
) -> LeadCommand | None:
    key = f"telegram:{update.bot_id}:{update.update_id}:{kind}"
    names = {
        "pause": "pause_goal",
        "resume": "resume_goal",
        "cancel": "cancel_goal",
        "revoke": "revoke_delegation",
        "status": "get_goal",
        "goal": "get_goal",
        "details": "get_goal",
        "debug": "get_goal",
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
    result: CommandResult,
    *,
    kind: str,
    user_id: int,
) -> str:
    from vuzol.leadership.card import (
        render_goal_card,
        render_goal_contract,
        render_goal_debug,
        render_goal_details,
    )
    from vuzol.leadership.persistence import GoalDelegation
    from vuzol.leadership.service import goal_card_view

    if result.status not in {"applied", "waiting", "duplicate"}:
        explanation = {
            "outside_scope": "Эта команда доступна только владельцу цели в её проекте.",
            "goal_not_found": "В этом проекте нет цели для этой команды.",
            "feature_disabled": "Руководитель отключён в настройках.",
            "question_version_mismatch": "Ответ относится к старому вопросу. Открой актуальный.",
            "owner_decision_required": "Нужно явное решение владельца.",
        }.get(result.reason or "", "Команду нельзя применить к текущему состоянию цели.")
        return (
            "🟡 **КОМАНДА НЕ ПРИМЕНЕНА · Руководитель**\n\n"
            f"{explanation}\n\n"
            "Открой текущее состояние цели."
        )
    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        goal = await uow.session.get(ManagedGoal, result.goal_id) if result.goal_id else None
        if goal is None or goal.project_id != project_id or goal.owner_user_id != user_id:
            return "**Руководитель**\n\nНет доступной цели для этой команды."
        delegation = await uow.session.get(GoalDelegation, goal.delegation_id)
        if delegation is None:
            return "**Руководитель**\n\nРазрешение на доступ к цели недоступно."
        view = await goal_card_view(uow.session, goal, delegation)
    renderer = {
        "goal": render_goal_contract,
        "details": render_goal_details,
        "debug": render_goal_debug,
    }.get(kind, render_goal_card)
    return renderer(view)


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


def question_options(question: str, supplied: object = None) -> list[str]:
    import re

    from vuzol.leadership.text import owner_text

    if isinstance(supplied, list):
        options = [owner_text(value) for value in supplied if isinstance(value, str)]
        if options and all(0 < len(value) <= 60 for value in options) and len(options) <= 6:
            return list(dict.fromkeys(options))
    if re.search(r"\bли\b", question, re.I) and question.rstrip().endswith("?"):
        return ["Да", "Нет"]
    return []


def question_buttons(goal: ManagedGoal) -> tuple[tuple[tuple[str, str], ...], ...]:
    condition = goal.wakeup_condition or {}
    options = condition.get("options", [])
    return tuple(
        tuple(
            (label, f"v1:lq:{condition['asked_version']}:{index}")
            for index, label in list(enumerate(options))[offset : offset + 2]
        )
        for offset in range(0, len(options), 2)
    )


async def answer_question_message(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    project_id: str,
    chat_id: int,
    thread_id: int,
    message_id: int,
    user_id: int,
    key: str,
    answer: str = "",
    question_version: int | None = None,
    option: int | None = None,
) -> IngressResult | None:
    """Resolve the delivered question, then use the same fenced owner command as /lead_answer."""
    from vuzol.storage.models import TelegramMessageLink

    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        link = await uow.session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.chat_id == chat_id,
                TelegramMessageLink.message_thread_id == thread_id,
                TelegramMessageLink.message_id == message_id,
                TelegramMessageLink.message_role.startswith("lq:"),
            )
        )
        if link is None:
            return None
        _, target, version_text = link.message_role.split(":")
        version = int(version_text)
        goal = await uow.session.get(ManagedGoal, uuid.UUID(target), with_for_update=True)
        if goal is None or goal.project_id != project_id or goal.owner_user_id != user_id:
            return IngressResult(status=IngressStatus.REJECTED, reason="outside_scope")
        if question_version is not None and question_version != version:
            return IngressResult(status=IngressStatus.REJECTED, reason="question_version_mismatch")
        if option is not None:
            notice = await uow.session.scalar(
                select(TransactionalOutbox).where(
                    TransactionalOutbox.idempotency_key == f"lead:question:{goal.id}:{version}"
                )
            )
            buttons = [
                button
                for row in (notice.payload.get("callback_buttons", []) if notice else [])
                for button in row
            ]
            if not 0 <= option < len(buttons):
                return IngressResult(status=IngressStatus.REJECTED, reason="answer_required")
            answer = buttons[option][0]
        receipt = await uow.session.scalar(
            select(LeadershipCommandReceipt).where(
                LeadershipCommandReceipt.actor_kind == "human",
                LeadershipCommandReceipt.actor_subject == str(user_id),
                LeadershipCommandReceipt.idempotency_key == key,
            )
        )
        result = await LeadershipCommandService(
            session_factory,
            enabled=settings.project_lead.enabled,
        ).apply_human(
            Principal(user_id=user_id, ingress_source="telegram"),
            LeadCommand(
                name="answer_permission",
                project_id=project_id,
                goal_id=goal.id,
                expected_version=receipt.expected_version if receipt else goal.version,
                idempotency_key=key,
                payload={"answer": answer, "question_version": version},
            ),
            uow=uow,
        )
        accepted = result.status in {"applied", "duplicate"}
        if not accepted:
            await enqueue_chat_notice(
                uow.session,
                chat_id=chat_id,
                thread_id=thread_id,
                key=f"lead:answer-rejected:{key}",
                text=(
                    "Этот вопрос уже закрыт или изменился. "
                    "Ответь на актуальное сообщение с вопросом."
                ),
            )
        return IngressResult(
            status=IngressStatus.HANDLED if accepted else IngressStatus.REJECTED,
            reason=result.reason or result.status,
        )


def goal_status_buttons(goal: ManagedGoal) -> tuple[tuple[tuple[str, str], ...], ...]:
    if goal.lifecycle != "paused":
        return ()
    return (
        (
            ("▶️ Продолжить", f"v1:lg:{goal.version}:resume"),
            ("❌ Отменить", f"v1:lg:{goal.version}:cancel"),
        ),
    )


def final_card_buttons(
    goal: ManagedGoal, *, ready: bool
) -> tuple[tuple[tuple[str, str], ...], ...]:
    if not ready:
        return ()
    return (
        (
            ("✅ Принять — влить в main", f"v1:lg:{goal.version}:accept"),
            ("✏️ Доработать", f"v1:lg:{goal.version}:rework"),
        ),
    )


async def apply_goal_button(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    project_id: str,
    update: ControlUpdate,
) -> IngressResult:
    from vuzol.storage.models import TelegramMessageLink

    async with UnitOfWork(session_factory) as uow:
        assert uow.session is not None
        link = await uow.session.scalar(
            select(TelegramMessageLink).where(
                TelegramMessageLink.chat_id == update.chat_id,
                TelegramMessageLink.message_thread_id == update.message_thread_id,
                TelegramMessageLink.message_id == update.message_id,
                or_(
                    TelegramMessageLink.message_role.startswith("ls:"),
                    TelegramMessageLink.message_role.startswith("lf:"),
                ),
            )
        )
        if link is None or update.goal_version is None:
            return IngressResult(status=IngressStatus.REJECTED, reason="control_binding_missing")
        goal_id = uuid.UUID(link.message_role[3:])
        if update.action_kind in {"lead_accept", "lead_rework"}:
            from vuzol.leadership.acceptance import queue_accept, request_rework
            from vuzol.leadership.fence import lock_project_admission
            from vuzol.leadership.service import enqueue_goal_status

            await lock_project_admission(uow.session, project_id)
            goal = await uow.session.get(ManagedGoal, goal_id, with_for_update=True)
            if goal is None or goal.project_id != project_id:
                return IngressResult(status=IngressStatus.REJECTED, reason="goal_not_available")
            if update.action_kind == "lead_accept" and goal.constraints.get("owner_acceptance"):
                return IngressResult(status=IngressStatus.DUPLICATE, reason="goal_accepted")
            if goal.version != update.goal_version:
                return IngressResult(status=IngressStatus.REJECTED, reason="stale_goal_card")
            if update.action_kind == "lead_accept":
                outcome = await queue_accept(uow.session, goal, user_id=update.user_id)
                if outcome == "accept_queued":
                    return IngressResult(status=IngressStatus.HANDLED, reason="goal_accept_queued")
                return IngressResult(status=IngressStatus.REJECTED, reason=outcome)
            outcome = await request_rework(uow.session, goal, user_id=update.user_id)
            if outcome == "rework_requested":
                await enqueue_goal_status(uow.session, goal)
                return IngressResult(status=IngressStatus.HANDLED, reason="goal_rework_requested")
            return IngressResult(status=IngressStatus.REJECTED, reason=outcome)
        result = await LeadershipCommandService(
            session_factory,
            enabled=settings.project_lead.enabled,
        ).apply_human(
            Principal(user_id=update.user_id, ingress_source="telegram"),
            LeadCommand(
                name="resume_goal" if update.action_kind == "lead_resume" else "cancel_goal",
                project_id=project_id,
                goal_id=goal_id,
                expected_version=update.goal_version,
                idempotency_key=f"telegram:lead-control:{update.callback_query_id}",
                payload={},
            ),
            uow=uow,
        )
        return IngressResult(
            status=IngressStatus.HANDLED
            if result.status in {"applied", "duplicate"}
            else IngressStatus.REJECTED,
            reason=result.reason or result.status,
        )
