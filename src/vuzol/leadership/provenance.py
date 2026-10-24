"""Trusted ingress provenance and a durable, lossless incoming-request notice."""
# ruff: noqa: RUF001

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.execution.artifacts import BUILTIN_SECRET_PATTERNS
from vuzol.leadership.persistence import ManagedGoal


class RequestOrigin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    channel: Literal["mcp", "telegram", "telegram_assistant", "think", "api", "cli"]
    display_name: str = Field(min_length=1, max_length=100)
    client_id: str | None = Field(default=None, max_length=300)
    request_id: str | None = Field(default=None, max_length=200)


def literal_block(value: str) -> str:
    # Request text cannot escape its code block and impersonate a system heading.
    for pattern in BUILTIN_SECRET_PATTERNS:
        value = re.sub(pattern, "[REDACTED]", value)
    longest = max((len(s) for s in re.findall(r"`+", value)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}\n{value}\n{fence}"


async def record_incoming_request(
    session: AsyncSession, goal: ManagedGoal, origin: RequestOrigin, *, historical: bool = False
) -> None:
    from vuzol.storage.models import Event
    from vuzol.telegram.projections import enqueue_chat_notice

    snapshot = origin.model_dump(mode="json")
    snapshot["authorized_user_id"] = goal.owner_user_id
    snapshot["historical_recovery"] = historical
    goal.constraints = {**goal.constraints, "request_origin": snapshot}
    session.add(
        Event(
            entity_type="managed_goal",
            entity_id=goal.id,
            event_type="managed_goal.request_recovered"
            if historical
            else "managed_goal.request_received",
            actor_type="owner",
            actor_id=str(goal.owner_user_id),
            payload={
                "origin": snapshot,
                "request": goal.original_request,
                "scope": goal.scope_text,
                "criteria": goal.criteria,
            },
        )
    )
    # A direct Telegram request is already visible. Only external ingress
    # needs a separate source-labelled copy; every ingress keeps its audit event.
    if origin.channel in {"telegram", "cli"}:
        return
    criteria = "\n".join(
        f"{i}. {item.get('description', '')}" for i, item in enumerate(goal.criteria, 1)
    )
    text = (
        (
            "📥 **ВОССТАНОВЛЕННОЕ ПОРУЧЕНИЕ**\n\n"
            "*Это запись истории, не новый запуск. Работа остаётся на паузе.*\n\n"
            if historical
            else "📥 **НОВОЕ ПОРУЧЕНИЕ**\n\n"
        )
        + f"**Источник:** {origin.display_name}\n"
        f"**Канал:** {origin.channel}\n"
        f"**Владелец:** `{goal.owner_user_id}`\n"
        f"**Проект:** `{goal.project_id}`\n"
        f"**Goal:** `{goal.id}`\n"
        + (f"**Request:** `{origin.request_id}`\n" if origin.request_id else "")
        + "\n**Что поступило в Вузол:**\n"
        + literal_block(goal.original_request)
        + "\n\n**Область работы и ограничения:**\n"
        + literal_block(goal.scope_text)
        + "\n\n**Критерии готовности:**\n"
        + literal_block(criteria)
    )
    await enqueue_chat_notice(
        session,
        chat_id=goal.source_chat_id,
        thread_id=goal.source_thread_id,
        key=f"managed-goal:{goal.id}:incoming-request",
        text=text,
        markdown=True,
    )
