"""Owner attention policy and durable projections over the existing Telegram outbox.

Open, link-refresh and resolved rows share a stable message role. The older
AttentionEvent policy below remains available to legacy notification callers.
"""

# ruff: noqa: RUF001

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from html import escape
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.leadership.persistence import GoalDelegation, ManagedGoal
from vuzol.leadership.rules import delegation_is_expired
from vuzol.leadership.text import short_owner_text
from vuzol.storage.models import TelegramMessageLink, TransactionalOutbox, WorkPackage
from vuzol.storage.types import WorkPackageStatus

# One owner-facing policy for goal waits, planner outcomes and managed task states.
# Unknown owner waits deliberately fall back to action, without exposing the reason code.
ATTENTION_REASONS = {
    "owner": ("🟡", "Нужен ответ на вопрос руководителя."),
    "plan_approval": ("🟡", "Нужно одобрить план работы."),
    "waiting_approval": ("🟡", "Нужно проверить и принять результат задачи."),
    "awaiting_user": ("🟡", "Нужно ответить, чтобы продолжить задачу."),
    "waiting": ("🟡", "Нужно твоё решение, чтобы продолжить работу."),
    "failed": ("🔴", "Работа завершилась с ошибкой. Нужно внимание."),
    "blocked": ("🔴", "Работа заблокирована. Нужно внимание."),
    "rolled_back": ("🔴", "Результат работы отменён из-за ошибки. Нужно внимание."),
    "unknown_effects_open": ("🔴", "Нужно проверить последствия внешней операции."),
    "lead_inference_reconciliation": ("🔴", "Нужно проверить исход обращения к модели."),
    "quota_exhausted": ("🔴", "Исчерпана квота модели. Нужно внимание."),
    "work_package_limit_reached": ("🔴", "Достигнут лимит работы. Нужно твоё решение."),
    "work_item_limit_reached": ("🔴", "Достигнут лимит задач. Нужно твоё решение."),
    "lead_budget_exceeded": ("🔴", "Достигнут лимит расходов. Нужно твоё решение."),
    "lead_context_limit": ("🔴", "Материалы не помещаются в контекст руководителя."),
    "goal_review_context_limit": ("🔴", "Материалы не помещаются в контекст проверки."),
    "lead_model_unconfigured": ("🔴", "Нужно подключить модель руководителя."),
    "lead_profile_unselected": ("🔴", "Нужно подключить доступную модель руководителя."),
    "lead_model_unavailable": ("🔴", "Модель руководителя недоступна. Нужно внимание."),
    "lead_response_invalid": ("🔴", "Не удалось получить пригодный ответ руководителя."),
    "goal_review_invalid": ("🔴", "Не удалось получить пригодное заключение проверки."),
    "goal_review_unavailable": ("🔴", "Проверка результата недоступна. Нужно внимание."),
    "planner_unavailable": ("🔴", "Модель планировщика недоступна. Нужно внимание."),
    "planner_model_unavailable": ("🔴", "Модель планировщика недоступна. Нужно внимание."),
    "planner_provider_unavailable": ("🔴", "Модель планировщика недоступна. Нужно внимание."),
    "planner_timeout": ("🔴", "Планировщик не ответил после повторных попыток."),
    "planner_authentication": ("🔴", "Нужно восстановить доступ к модели планировщика."),
    "planner_unknown": ("🔴", "Планировщик завершил работу с ошибкой. Нужно внимание."),
    "planner_invalid_output": ("🔴", "Не удалось получить пригодный план работы."),
    "planner_invalid_structured_output": ("🔴", "Не удалось получить пригодный план работы."),
    "planner_output_truncated": ("🔴", "Планировщик не смог вернуть полный план работы."),
    "planner_context_too_large": ("🔴", "Материалы не помещаются в контекст планировщика."),
    "planner_unsupported_capability": (
        "🔴",
        "Модель планировщика не поддерживает нужное действие.",
    ),
    "planner_permanent_request": ("🔴", "Модель планировщика отклонила запрос. Нужно внимание."),
    "planner_profile_unselected": ("🔴", "Нужно подключить доступную модель планировщика."),
    "planner_budget_exceeded": ("🔴", "Достигнут лимит расходов на планирование."),
    "planner_quota_exhausted": ("🔴", "Исчерпана квота планировщика. Нужно внимание."),
    "planner_rate_limited": ("🔴", "Достигнут лимит запросов планировщика."),
    "planner_context_limit": ("🔴", "Материалы не помещаются в контекст планировщика."),
    "delegation_expired": ("🔴", "Истёк срок разрешения на работу. Нужно твоё решение."),
    "delegation_revoked": ("🔴", "Разрешение на работу отозвано."),
    "completed": ("✅", "Цель выполнена. Результат подтверждён."),
}


def attention_text(reason: str, topic_name: str, *, resolved: bool = False) -> str:
    icon, line = ATTENTION_REASONS.get(reason, ATTENTION_REASONS["waiting"])
    name = escape(short_owner_text(topic_name, 100) or "Проект")
    return f"✅ {name}\nрешено: {line}" if resolved else f"{icon} {name}\n{line}"


def project_message_url(chat_id: int, thread_id: int, message_id: int | None = None) -> str:
    internal = str(chat_id).removeprefix("-100")
    return f"https://t.me/c/{internal}/{thread_id}" + (
        f"/{message_id}" if message_id is not None else ""
    )


async def _enqueue(
    session: AsyncSession, goal_id: uuid.UUID, key: str, payload: dict[str, Any]
) -> None:
    if (
        await session.scalar(
            select(TransactionalOutbox.id).where(
                TransactionalOutbox.destination == "telegram",
                TransactionalOutbox.idempotency_key == key,
            )
        )
        is None
    ):
        session.add(
            TransactionalOutbox(
                destination="telegram",
                operation_type="send_message",
                linked_entity_type="managed_goal",
                linked_entity_id=goal_id,
                idempotency_key=key,
                payload=payload,
            )
        )
        await session.flush()


async def _lock(session: AsyncSession, goal_id: uuid.UUID) -> None:
    # Task and goal producers share this lock without acquiring each other's row locks.
    await session.execute(
        select(func.pg_advisory_xact_lock(func.hashtextextended(f"attention:{goal_id}", 0)))
    )


async def sync_attention(
    session: AsyncSession,
    goal: ManagedGoal,
    reason: str | None,
    *,
    scope: str = "goal",
    decision: str | None = None,
    target: dict[str, str] | None = None,
    resolve_all: bool = False,
    version: int | None = None,
) -> None:
    """Append an immutable open/resolved projection in the state-changing transaction.

    Outbox keys and confirmed message links are the durable item ledger. A changing
    status version alone does not create another item for an unresolved situation.
    """
    if goal.source_chat_id is None or goal.source_thread_id is None:
        return
    await _lock(session, goal.id)
    rows = list(
        await session.scalars(
            select(TransactionalOutbox).where(
                TransactionalOutbox.destination == "telegram",
                TransactionalOutbox.linked_entity_id == goal.id,
                TransactionalOutbox.payload["role"].astext == "attention",
            )
        )
    )
    resolved = {row.payload["projection_role"] for row in rows if row.payload.get("resolved")}
    found = False
    for row in rows:
        data = row.payload
        if data["revision"] != 1 or data["projection_role"] in resolved:
            continue
        if not resolve_all and data["scope"] != scope:
            continue
        if (
            reason is not None
            and data["scope"] == scope
            and data["reason"] == reason
            and (decision is None or data["decision"] == decision)
        ):
            found = True
            continue
        # Completion is an informational history entry, never an unresolved decision.
        if data["reason"] != "completed":
            await _enqueue(
                session,
                goal.id,
                data["projection_role"] + ":resolved",
                {**data, "resolved": True, "revision": 3},
            )
    if reason is None or found:
        return
    decision_version = decision or str(goal.version if version is None else version)
    identity = uuid.uuid5(goal.id, f"{scope}:{reason}:{decision_version}")
    reopened = 1
    while f"la:{identity.hex}" in resolved:
        # The same situation came back after it was resolved: a new item, not a no-op.
        reopened += 1
        identity = uuid.uuid5(goal.id, f"{scope}:{reason}:{decision_version}:{reopened}")
    await _enqueue(
        session,
        goal.id,
        f"la:{identity.hex}",
        {
            "role": "attention",
            "projection_role": f"la:{identity.hex}",
            "revision": 1,
            "reason": reason,
            "scope": scope,
            "decision": decision_version,
            "resolved": False,
            "chat_id": goal.source_chat_id,
            "project_thread_id": goal.source_thread_id,
            "project_id": goal.project_id,
            "target": target or {"message_role": f"ls:{goal.id.hex}"},
        },
    )


async def sync_goal_attention(
    session: AsyncSession, goal: ManagedGoal, *, completion_confirmed: bool = False
) -> None:
    reason = None
    decision = None
    target = None
    if goal.lifecycle == "completed":
        reason = "completed" if completion_confirmed else "waiting"
        decision = "completed"
    elif goal.lifecycle in {"waiting", "paused"}:
        reason = goal.waiting_reason or "waiting"
        if reason in {"wait_for", "owner_pause"}:
            reason = None
    elif goal.lifecycle in {"failed", "blocked"}:
        reason = goal.lifecycle
    if goal.lifecycle not in {"completed", "cancelled"}:
        delegation = await session.get(GoalDelegation, goal.delegation_id)
        if delegation is not None:
            if delegation.revoked_at is not None:
                reason = "delegation_revoked"
            elif delegation_is_expired(delegation.expires_at, now=datetime.now(UTC)):
                reason = "delegation_expired"
    condition = goal.wakeup_condition or {}
    # A draft only the owner can approve stays his decision whatever the goal's
    # waiting reason says: the lead may call it wait_for, or the owner may pause.
    pending_plan = await pending_owner_plan(session, goal)
    if pending_plan is not None and reason not in {"delegation_revoked", "delegation_expired"}:
        reason = "plan_approval"
    if reason == "plan_approval":
        if pending_plan is not None:
            package, decision = pending_plan
            target = {
                "work_package_id": str(package.id),
                "plan_revision_id": decision,
                "message_role": "work_package_plan",
            }
        else:
            reason = None
    elif reason is not None and condition.get("kind") == "owner":
        decision = str(condition.get("asked_version", goal.version))
        target = {"message_role": f"lq:{goal.id.hex}:{decision}"}
    await sync_attention(
        session,
        goal,
        reason,
        decision=decision,
        target=target,
        resolve_all=goal.lifecycle in {"cancelled", "completed", "parked"}
        or (goal.lifecycle == "paused" and reason != "plan_approval"),
    )


async def pending_owner_plan(
    session: AsyncSession, goal: ManagedGoal
) -> tuple[WorkPackage, str] | None:
    from vuzol.leadership.batches import revision_needs_owner
    from vuzol.storage.models import PlanRevision
    from vuzol.storage.types import PlanRevisionState

    if goal.lifecycle in {"completed", "cancelled", "parked"} or goal.current_package_id is None:
        return None
    package = await session.get(WorkPackage, goal.current_package_id)
    # A goal pause also pauses its draft package; the revision state is what
    # tells whether the owner still has to decide.
    if (
        package is None
        or package.status not in {WorkPackageStatus.DRAFT, WorkPackageStatus.PAUSED}
        or package.head_revision_id is None
    ):
        return None
    revision = await session.get(PlanRevision, package.head_revision_id)
    if (
        revision is None
        or revision.state is not PlanRevisionState.DRAFT
        or not await revision_needs_owner(session, revision)
    ):
        return None
    return package, str(revision.id)


def matches_target(payload: dict[str, Any], link: TelegramMessageLink) -> bool:
    target = payload["target"]
    return (
        link.chat_id == payload["chat_id"]
        and link.message_thread_id == payload["project_thread_id"]
        and all(str(getattr(link, key)) == value for key, value in target.items())
    )


async def refresh_attention_links(session: AsyncSession, link: TelegramMessageLink) -> None:
    """A confirmed project send may arrive after its attention item used a thread link."""
    rows = list(
        await session.scalars(
            select(TransactionalOutbox).where(
                TransactionalOutbox.destination == "telegram",
                TransactionalOutbox.payload["role"].astext == "attention",
                TransactionalOutbox.payload["revision"].as_integer() == 1,
                TransactionalOutbox.payload["chat_id"].astext == str(link.chat_id),
                TransactionalOutbox.payload["project_thread_id"].astext
                == str(link.message_thread_id),
            )
        )
    )
    for row in rows:
        if matches_target(row.payload, link):
            await _lock(session, row.linked_entity_id)
            await _enqueue(
                session,
                row.linked_entity_id,
                row.payload["projection_role"] + ":link",
                {**row.payload, "revision": 2},
            )


class AttentionEvent(StrEnum):
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    APPROVAL_NEEDED = "approval_needed"
    ATTENTION_FLAGGED = "attention_flagged"
    PACKAGE_COMPLETED = "package_completed"
    PACKAGE_ATTENTION = "package_attention"
    RETRY_SCHEDULED = "retry_scheduled"
    BACKOFF_DEFERRED = "backoff_deferred"
    QUEUE_SHUFFLED = "queue_shuffled"
    STATE_UNCHANGED = "state_unchanged"
    DUPLICATE_DELIVERY = "duplicate_delivery"


_MEANINGFUL = frozenset(
    {
        AttentionEvent.TASK_COMPLETED,
        AttentionEvent.TASK_FAILED,
        AttentionEvent.APPROVAL_NEEDED,
        AttentionEvent.ATTENTION_FLAGGED,
        AttentionEvent.PACKAGE_COMPLETED,
        AttentionEvent.PACKAGE_ATTENTION,
    }
)


def should_notify(event: AttentionEvent) -> bool:
    """True only for meaningful transitions; transient mechanics stay silent."""

    return event in _MEANINGFUL
