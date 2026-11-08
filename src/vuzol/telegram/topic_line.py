"""The pinned line of a project topic: is anything happening there right now.

Owner 2026-11-08: only the first line of the pinned message shows, about 30
characters on a phone. It must answer one question; the project status and
attention topics carry the rest. So it names what runs now (a lead, worker or
reviewer sandbox) with a short model·account label, e.g. «🔨 Код · ds·oc-c».
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.leadership.persistence import (
    LIVE_GOAL_LIFECYCLES,
    LeadershipInference,
    LeadershipTurn,
    ManagedGoal,
)
from vuzol.storage.models import Step, SupervisedProcess, Task
from vuzol.storage.types import ProcessStatus

# Model families, longest prefix first.
_FAMILIES = (
    ("deepseek", "ds"),
    ("muse-spark", "muse"),
    ("muse", "muse"),
    ("mimo", "mimo"),
    ("kimi", "kimi"),
    ("grok", "grok"),
    ("glm", "glm"),
    ("qwen", "qwen"),
    ("claude", "claude"),
)
_CODEX = re.compile(r"^gpt-[\d.]+-(sol|terra|luna)$")
_ACCOUNTS = (("pi-opencode-go-", "oc-"), ("opencode-", "oc-"), ("codex-", "cx-"), ("grok-", "gk-"))
_STEPS = {
    "execute_agent": "🔨 Код",
    "execute_code": "🔨 Код",
    "execute_model": "🔨 Работа",
    "review": "🔍 Ревью",
    "validate": "🧪 Тесты",
    "prepare_worktree": "📦 Готовит",
    "ensure_dependencies": "📦 Готовит",
}
_PHASES = {"lead": "🧭 Лид думает", "planner": "🗺 План", "goal_review": "🔍 Проверка"}


def _family(model: str) -> str:
    slug = model.strip().lower()
    match = _CODEX.match(slug)
    if match:
        return match.group(1)
    for prefix, short in _FAMILIES:
        if slug.startswith(prefix):
            return short
    return slug.split("-")[0][:8] or "?"


def _version(model: str) -> str:
    """deepseek-v4.1-flash → 4.1f; muse-spark-1.3-contributor → 1.3."""
    slug = model.strip().lower()
    match = re.search(r"v?(\d+(?:\.\d+)?)", slug)
    version = match.group(1) if match else ""
    for tier, mark in (("flash", "f"), ("pro", "p"), ("mini", "m")):
        if tier in slug.split("-"):
            return version + mark
    return version


def model_aliases(models: Iterable[str]) -> dict[str, str]:
    """Short names; a version is added only when a family has more than one model."""
    known = sorted({m for m in models if m})
    families: dict[str, list[str]] = {}
    for model in known:
        families.setdefault(_family(model), []).append(model)
    aliases = {}
    for family, members in families.items():
        for model in members:
            version = _version(model) if len(members) > 1 else ""
            aliases[model] = f"{family}-{version}" if version else family
    return aliases


def account_alias(profile_id: str) -> str:
    """pi-opencode-go-c-muse → oc-c; codex-a → cx-a."""
    for prefix, short in _ACCOUNTS:
        if profile_id.startswith(prefix):
            letter = profile_id.removeprefix(prefix).split("-")[0]
            return short + letter
    return profile_id[:8]


def profile_labels(profiles: Iterable[ProviderProfileConfig]) -> dict[str, str]:
    items = list(profiles)
    aliases = model_aliases(p.model for p in items)
    return {p.id: f"{aliases.get(p.model, _family(p.model))}·{account_alias(p.id)}" for p in items}


async def topic_line(session: AsyncSession, project_id: str, labels: Mapping[str, str]) -> str:
    from sqlalchemy import func

    from vuzol.storage.models import ProjectTaskQueueItem

    queued = await session.scalar(
        select(func.count())
        .select_from(ProjectTaskQueueItem)
        .where(
            ProjectTaskQueueItem.project_id == project_id,
            ProjectTaskQueueItem.state == "queued",
        )
    )
    line = await _now_line(session, project_id, labels)
    return f"{line} · +{queued}" if queued else line


async def _now_line(session: AsyncSession, project_id: str, labels: Mapping[str, str]) -> str:
    running: list[tuple[str, str]] = []
    rows = await session.execute(
        select(Step.step_type, SupervisedProcess.profile_id)
        .join(Task, Task.id == SupervisedProcess.task_id)
        .join(Step, Step.id == SupervisedProcess.step_id)
        .where(
            Task.project_id == project_id,
            SupervisedProcess.status.in_((ProcessStatus.STARTING, ProcessStatus.RUNNING)),
            SupervisedProcess.reaped_at.is_(None),
        )
    )
    running += [(_STEPS.get(kind, "⚙️ Работа"), profile) for kind, profile in rows.all()]
    rows = await session.execute(
        select(LeadershipInference.phase, SupervisedProcess.profile_id)
        .join(LeadershipTurn, LeadershipTurn.id == SupervisedProcess.leadership_turn_id)
        .join(ManagedGoal, ManagedGoal.id == LeadershipTurn.goal_id)
        .join(LeadershipInference, LeadershipInference.id == SupervisedProcess.invocation_id)
        .where(
            ManagedGoal.project_id == project_id,
            SupervisedProcess.status.in_((ProcessStatus.STARTING, ProcessStatus.RUNNING)),
            SupervisedProcess.reaped_at.is_(None),
        )
    )
    running += [(_PHASES.get(phase, "🧭 Лид думает"), profile) for phase, profile in rows.all()]
    if running:
        # Work beats thinking: a worker running says more than the lead waiting on it.
        order = list(_STEPS.values()) + list(_PHASES.values())
        running.sort(key=lambda item: order.index(item[0]) if item[0] in order else len(order))
        action, profile = running[0]
        same = sum(1 for other, _ in running if other == action)
        count = f" ×{same}" if same > 1 else ""  # noqa: RUF001 - a count, as the owner reads it
        label = labels.get(profile, account_alias(profile))
        return f"{action}{count} · {label}"
    goals = list(
        await session.scalars(
            select(ManagedGoal)
            .where(ManagedGoal.project_id == project_id)
            .where(
                ManagedGoal.lifecycle.in_((*LIVE_GOAL_LIFECYCLES, "completed")),
            )
            .order_by(ManagedGoal.created_at.desc())
            .limit(5)
        )
    )
    for goal in goals:
        if goal.lifecycle == "completed" and not goal.constraints.get("owner_acceptance"):
            return "✅ Готово, прими"
        if goal.lifecycle == "paused":
            return "⏸ Пауза"
        if goal.lifecycle == "draft" or (
            goal.lifecycle == "waiting"
            and (goal.wakeup_condition or {}).get("kind") in {"owner", None}
            and goal.waiting_reason not in {"wait_for"}
        ):
            return "⏳ Ждёт тебя"
        if goal.lifecycle in {"active", "waiting"}:
            return "🎯 В работе"  # noqa: RUF001 - Russian text
    return "💤 Свободен"


def line_revision(line: str) -> int:
    """A stable, positive int for the link's projection_revision: equal lines never re-edit."""
    return int(hashlib.sha256(line.encode()).hexdigest()[:8], 16) % (2**31 - 1) or 1


async def refresh_topic_lines(session: AsyncSession, labels: Mapping[str, str]) -> int:
    """Queue an edit for every project topic whose pinned line changed; returns how many."""
    import uuid
    from html import escape

    from vuzol.storage.models import TelegramMessageLink, TopicMapping, TransactionalOutbox
    from vuzol.storage.types import DeliveryStatus
    from vuzol.telegram.work_package_projections import (
        WORK_PACKAGE_PROJECTION_DESTINATION,
        WORK_PACKAGE_STATUS_ROLE,
    )

    queued = 0
    mappings = await session.scalars(
        select(TopicMapping).where(
            TopicMapping.topic_kind == "project",
            TopicMapping.enabled.is_(True),
            TopicMapping.project_id.is_not(None),
        )
    )
    for mapping in list(mappings):
        assert mapping.project_id is not None
        line = await topic_line(session, mapping.project_id, labels)
        revision = line_revision(line)
        shown = await session.scalar(
            select(TelegramMessageLink.projection_revision).where(
                TelegramMessageLink.chat_id == mapping.chat_id,
                TelegramMessageLink.message_thread_id == mapping.message_thread_id,
                TelegramMessageLink.message_role == WORK_PACKAGE_STATUS_ROLE,
            )
        )
        if shown == revision:
            continue
        on_its_way = await session.scalar(
            select(TransactionalOutbox.id)
            .where(
                TransactionalOutbox.destination == WORK_PACKAGE_PROJECTION_DESTINATION,
                TransactionalOutbox.operation_type.in_(
                    ("render_topic_line", "render_topic_status", "render_topic_idle")
                ),
                TransactionalOutbox.status.in_((DeliveryStatus.PENDING, DeliveryStatus.LEASED)),
                TransactionalOutbox.payload["chat_id"].astext == str(mapping.chat_id),
                TransactionalOutbox.payload["thread_id"].astext == str(mapping.message_thread_id),
            )
            .order_by(TransactionalOutbox.created_at.desc())
            .limit(1)
        )
        if on_its_way is not None:
            item = await session.get(TransactionalOutbox, on_its_way)
            if item is not None and item.payload.get("revision") == revision:
                continue
            if item is not None and item.operation_type != "render_topic_line":
                continue  # the topic's first card is still being sent
        session.add(
            TransactionalOutbox(
                destination=WORK_PACKAGE_PROJECTION_DESTINATION,
                operation_type="render_topic_line",
                linked_entity_type="topic_mapping",
                linked_entity_id=mapping.id,
                idempotency_key=f"topic-line:{mapping.chat_id}:{mapping.message_thread_id}:"
                f"{uuid.uuid4().hex}",
                payload={
                    "chat_id": mapping.chat_id,
                    "thread_id": mapping.message_thread_id,
                    "project_id": mapping.project_id,
                    "revision": revision,
                    "html": f"<b>{escape(line)}</b>",
                },
                status=DeliveryStatus.PENDING,
            )
        )
        queued += 1
    await session.flush()
    return queued
