"""Receipt-bound corrections to a lead's earlier conversational reply."""
# ruff: noqa: RUF001

import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import LeadershipCommandReceipt, ManagedGoal
from vuzol.storage.models import TransactionalOutbox
from vuzol.telegram.projections import enqueue_chat_notice

_REASONS = {
    "goal_paused": "цель на паузе; запуск работы запрещён",
    "goal_waiting": "цель ожидает ответа владельца",
    "goal_terminal": "цель уже завершена или отменена",
    "goal_version_mismatch": "состояние цели изменилось после подготовки ответа",
    "stale_version": "состояние цели изменилось после подготовки ответа",
    "expected_version_mismatch": "состояние цели изменилось после подготовки ответа",
    "action_not_delegated": "владелец не делегировал это действие",
    "delegation_revoked": "владелец отозвал разрешение",
    "delegation_expired": "срок разрешения владельца истёк",
    "unsupported_command": "эта команда не поддерживается",
    "unknown_effects_open": "не выяснены последствия операции вне рабочей копии задачи",
    "amendment_pending": "предыдущее изменение цели ещё ожидает применения",
    "project_task_running": "в проекте ещё есть незавершённая задача",
    "package_still_live": "текущий план ещё выполняется или стоит на паузе",
    "task_spec_stale": "пункт плана или версия цели изменились",
    "result_revision_stale": "результат относится к другой версии задания или плана",
    "result_commit_stale": "commit результата не совпадает с проверенным",
    "result_not_ready": "задача ещё не готова к приёмке",
    "validation_or_repair_pending": "проверки или исправления задачи ещё не закончены",
    "feature_disabled": "управление задачами через лида отключено",
}


async def command_notice(
    session: AsyncSession,
    receipt: LeadershipCommandReceipt,
    wakeup_id: uuid.UUID | None,
    goal: ManagedGoal | None,
) -> None:
    if wakeup_id is None:
        return
    source = await session.get(TransactionalOutbox, wakeup_id)
    if source is None or source.payload.get("wakeup_kind") != "owner_message":
        return
    text = None
    if receipt.status in {"denied", "stale", "unsupported"}:
        reason = _REASONS.get(receipt.reason or "")
        if reason is None:
            reason = (
                "решение устарело: состояние цели изменилось"
                if receipt.status == "stale"
                else "команда не прошла проверку условий выполнения"
            )
        text = f"Уточнение к моему ответу: действие не выполнено — {reason}."
    elif receipt.command_name in {"amend_goal", "request_revision"} and goal is not None:
        amendment = goal.constraints.get("pending_amendment") or goal.constraints.get(
            "queued_amendment"
        )
        if amendment:
            from vuzol.leadership.evidence import package_has_unknown_effects

            external = goal.current_package_id is not None and await package_has_unknown_effects(
                session, goal.current_package_id
            )
            reason = (
                "не выяснены последствия операции вне рабочей копии задачи"
                if external
                else "нужно дождаться завершения текущего плана"
                if amendment["inflight_policy"] == "queue"
                else "нужно дождаться остановки текущей задачи и сверки состояния"
            )
            text = (
                "Уточнение к моему ответу: изменение цели сохранено, "
                f"но ещё не применено — {reason}."
            )
    elif receipt.status == "waiting":
        text = (
            "Уточнение к моему ответу: выполнение ожидает ответа владельца "
            "или завершения текущей работы."
        )
    if text:
        await enqueue_chat_notice(
            session,
            chat_id=source.payload.get("chat_id"),
            thread_id=source.payload.get("message_thread_id"),
            key=f"lead:command-notice:{receipt.id}",
            text=text,
        )


async def reject_saved_commands(
    session: AsyncSession,
    goal: ManagedGoal,
    wakeup_id: uuid.UUID,
    decision: dict[str, Any],
    reason: str,
) -> None:
    from sqlalchemy import select

    from vuzol.leadership.actor import Actor, server_agent_principal
    from vuzol.leadership.commands import CommandStatus, LeadCommand, payload_hash
    from vuzol.leadership.service import _Outcome, _store

    source = await session.get(TransactionalOutbox, wakeup_id)
    if source is None or source.payload.get("wakeup_kind") != "owner_message":
        return
    actor = Actor.agent(server_agent_principal(goal.project_id), "worker")
    for raw in decision.get("commands", []):
        key = raw["idempotency_key"]
        existing = await session.scalar(
            select(LeadershipCommandReceipt).where(
                LeadershipCommandReceipt.actor_kind == "agent",
                LeadershipCommandReceipt.actor_subject == actor.subject,
                LeadershipCommandReceipt.idempotency_key == key,
            )
        )
        if existing is not None:
            continue
        command = LeadCommand(
            name=raw["name"],
            project_id=goal.project_id,
            goal_id=goal.id,
            expected_version=raw["expected_version"],
            idempotency_key=key,
            payload=raw["payload"],
            reason=raw.get("reason", ""),
        )
        await _store(
            session,
            actor,
            command,
            payload_hash(command),
            _Outcome(CommandStatus.STALE, reason, {}, goal),
            source_wakeup_id=wakeup_id,
        )


async def planner_failure_notice(
    session: AsyncSession, goal: ManagedGoal, key: str, reason: str
) -> None:
    from vuzol.storage.models import ProjectDiscussionSession

    topic = await session.get(ProjectDiscussionSession, goal.discussion_session_id)
    if topic is None:
        return
    reasons = {
        "planner_budget_exceeded": "достигнут лимит бюджета или параллельных вызовов",
        "planner_quota_exhausted": "исчерпана квота модели",
        "planner_rate_limited": "достигнут лимит запросов модели",
        "planner_profile_unselected": "профиль планировщика недоступен",
        "planner_head_changed": "исходный commit изменился во время планирования",
        "planner_scope_changed": "цель изменилась во время планирования",
        "planner_invalid_output": "модель вернула некорректный план",
        "planner_output_truncated": "ответ модели был обрезан",
        "current_task_unfinished": "текущая задача ещё не завершена",
        "source_repository_unavailable": "не удалось безопасно прочитать исходный commit",
        "planner_context_limit": "контекст превышает лимит планировщика",
        "planner_revision_changed": "ревизия плана изменилась во время планирования",
        "planner_invalid_structured_output": "модель вернула некорректный ответ",
        "amendment_pending": "изменение цели ещё ожидает завершения текущей работы",
    }
    detail = reasons.get(reason, "планировщик не смог подготовить план")
    await enqueue_chat_notice(
        session,
        chat_id=topic.chat_id,
        thread_id=topic.message_thread_id,
        key=f"{key}:notice",
        text=f"План не подготовлен: {detail} ({reason}). Лид может повторить запрос позже.",
    )
