"""Reconstructable and revision-safe Telegram projections."""
# ruff: noqa: RUF001

import asyncio
import hashlib
import html
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.projects.executor_preference import format_preference_label, load_preference
from vuzol.providers.subscription_limits import (
    SubscriptionLimitSnapshot,
    format_subscription_limits_html,
    load_subscription_limits,
)
from vuzol.storage.models import (
    Approval,
    Event,
    MaterializationLink,
    PlanRevision,
    PlanRevisionItem,
    Run,
    Step,
    Task,
    TelegramIntakeMessage,
    TelegramMessageLink,
    TopicMapping,
    TransactionalOutbox,
    UsageRecord,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import (
    USER_REPORTABLE_TASK_STATUSES,
    USER_TERMINAL_TASK_STATUSES,
    ApprovalStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.telegram.layout import (
    DASHBOARD_CARD_TITLE,
    HISTORY_TOPIC_KIND,
    STATUS_DASHBOARD_TOPIC_KIND,
)
from vuzol.workflows.result_approval import verified_envelope

TELEGRAM_TEXT_LIMIT = 4096
# Outbox/message_role for the single editable card in the task_dashboard topic.
PROJECT_STATUS_DASHBOARD_ROLE = "project_status_dashboard"
# One-shot completion report in the «История» (changelog) topic.
TASK_HISTORY_ROLE = "task_history"

_ACTIVE_PROVIDER_STEPS = frozenset(
    {
        "plan",
        "execute_model",
        "execute_code",
        "execute_agent",
        "research_execute",
        "synthesize",
        "review",
    }
)
_WORKER_STEP_TYPES = frozenset(
    {"execute_model", "execute_code", "execute_agent", "research_execute", "synthesize"}
)


def telegram_html(value: object) -> str:
    """Escape all externally supplied text before using Telegram HTML mode."""

    return html.escape(str(value), quote=True)


def split_message(text: str, limit: int = TELEGRAM_TEXT_LIMIT) -> tuple[str, ...]:
    if limit < 1:
        raise ValueError("message limit must be positive")
    return tuple(text[offset : offset + limit] for offset in range(0, len(text), limit)) or ("",)


@dataclass(frozen=True, slots=True)
class StatusCard:
    task_id: uuid.UUID
    revision: int
    html: str
    buttons: tuple[str, ...] = ()
    approval_id: uuid.UUID | None = None
    callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = ()
    work_package_id: uuid.UUID | None = None
    plan_revision_id: uuid.UUID | None = None
    control_status_generation: int | None = None


@dataclass(frozen=True, slots=True)
class DashboardCard:
    """Single editable global dashboard projection for one forum chat."""

    chat_id: int
    revision: int
    html: str


def task_title(task: Task) -> str:
    if task.public_task_number is not None:
        return f"Задача №{task.public_task_number}"
    return str(
        task.task_draft.get("normalized_title")
        or task.task_draft.get("title")
        or task.original_text
    ).strip()[:120]


def task_number_label(task: Task) -> str:
    if task.public_task_number is not None:
        return str(task.public_task_number)
    if task.topic_task_number is not None:
        return f"{task.topic_task_number:04d}"
    return f"·{task.id.hex[-8:]}"


_TASK_STATUS_LABELS: dict[TaskStatus, str] = {
    TaskStatus.RECEIVED: "Получена",
    TaskStatus.INTERPRETED: "Разобрана",
    TaskStatus.CONTEXT_PREPARED: "Контекст готов",
    TaskStatus.PLANNED: "План готов",
    TaskStatus.WAITING_APPROVAL: "Ждёт решения",
    TaskStatus.EXECUTING: "Выполняется",
    TaskStatus.VALIDATING: "Проверяется",
    TaskStatus.REVIEWING: "На ревью",
    TaskStatus.AWAITING_USER: "Ждёт ответа",
    TaskStatus.PAUSED: "На паузе",
    TaskStatus.RETRYING: "Повтор выполнения",
    TaskStatus.QUOTA_EXHAUSTED: "Лимит исчерпан",
    TaskStatus.BLOCKED: "Заблокирована",
    TaskStatus.FAILED: "Ошибка",
    TaskStatus.CANCELLED: "Отменена",
    TaskStatus.ROLLED_BACK: "Откат",
    TaskStatus.COMPLETED: "Завершена",
}

_STEP_STATUS_LABELS: dict[StepStatus, str] = {
    StepStatus.PENDING: "Ожидает",
    StepStatus.QUEUED: "В очереди",
    StepStatus.LEASED: "Захвачен",
    StepStatus.RUNNING: "Идёт",
    StepStatus.WAITING_APPROVAL: "Ждёт решения",
    StepStatus.AWAITING_USER: "Ждёт пользователя",
    StepStatus.BLOCKED: "Блок",
    StepStatus.FAILED: "Ошибка",
    StepStatus.CANCELLED: "Отменён",
    StepStatus.COMPLETED: "Готово",
}

_STEP_TYPE_LABELS = {
    "interpret": "Интерпретация",
    "execute_model": "Вызов модели",
    "format_result": "Форматирование результата",
    "finalize": "Завершение",
    "plan": "Планирование",
    "prepare_context": "Подготовка контекста",
    "prepare_worktree": "Подготовка рабочей копии",
    "execute_code": "Выполнение кода",
    "validate": "Проверка",
    "review": "Ревью",
    "build_static": "Сборка сайта",
    "approval": "Решение / апрув",
    "publish_static": "Публикация прототипа",
    "publish_preview": "Публикация preview",
    "execute_agent": "Агент",
    "research_execute": "Исследование",
    "synthesize": "Синтез",
    "inspect": "Инспекция",
    "privileged_execute": "Привилегированное выполнение",
    "complete_or_block": "Завершение или блок",
}

_DELIVERY_STATE_LABELS: dict[WorktreeDeliveryState, str] = {
    WorktreeDeliveryState.ACTIVE: "активна",
    WorktreeDeliveryState.WORKTREE_RETAINED: "рабочая копия сохранена",
    WorktreeDeliveryState.PATCH_DELIVERED: "патч доставлен",
    WorktreeDeliveryState.APPLIED: "применено",
    WorktreeDeliveryState.MERGED: "влито",
    WorktreeDeliveryState.PUSHED: "отправлено в remote",
    WorktreeDeliveryState.CLEANED: "очищено",
}


def user_status_label(status: TaskStatus | str) -> str:
    try:
        known = status if isinstance(status, TaskStatus) else TaskStatus(status)
    except ValueError:
        return telegram_html(status)
    return _TASK_STATUS_LABELS[known]


def step_status_label(status: StepStatus | str) -> str:
    try:
        known = status if isinstance(status, StepStatus) else StepStatus(status)
    except ValueError:
        return telegram_html(status)
    return _STEP_STATUS_LABELS[known]


def step_type_label(step_type: str) -> str:
    return telegram_html(_STEP_TYPE_LABELS.get(step_type, step_type))


def delivery_state_label(state: WorktreeDeliveryState | str) -> str:
    try:
        known = state if isinstance(state, WorktreeDeliveryState) else WorktreeDeliveryState(state)
    except ValueError:
        return telegram_html(state)
    return _DELIVERY_STATE_LABELS[known]


def _task_identity_footer(task: Task) -> str | None:
    if task.public_task_number is not None:
        return None
    if task.topic_task_number is not None:
        return f"<i>локальный №{task.topic_task_number:04d}</i>"
    return f"<i>код ·{task.id.hex[-8:]}</i>"


def task_sense_sentence(task: Task) -> str:
    """One short user-facing sentence about what the task is for."""

    draft = task.task_draft if isinstance(task.task_draft, dict) else {}
    raw = (
        draft.get("task_summary")
        or draft.get("normalized_title")
        or draft.get("goal")
        or draft.get("title")
        or task.original_text
        or ""
    )
    text = " ".join(str(raw).split()).strip()
    if not text:
        return "Без описания"
    for separator in (". ", "! ", "? ", "\n"):
        if separator in text:
            text = text.split(separator, 1)[0].strip()
            break
    text = text.rstrip(".!?").strip()
    if len(text) > 160:
        text = text[:157].rstrip() + "…"
    return text or "Без описания"


def model_label_for_profile(
    profile_id: str | None,
    *,
    profile_models: Mapping[str, str] | None = None,
    profile_efforts: Mapping[str, str | None] | None = None,
    profile_providers: Mapping[str, str] | None = None,
    model: str | None = None,
    effort: str | None = None,
) -> str:
    """Human-readable executor identity with full model + effort when known.

    Prefer explicit ``model``/``effort`` (from a step/usage record). Fall back to
    the profile registry mapping so the dashboard still shows the configured
    executor (e.g. ``Codex Sol · medium``) before the first step result lands.
    """

    if not profile_id and not model:
        return "ещё не назначен"
    resolved_model = model
    if resolved_model is None and profile_id and profile_models is not None:
        resolved_model = profile_models.get(profile_id)
    resolved_effort = effort
    if resolved_effort is None and profile_id and profile_efforts is not None:
        resolved_effort = profile_efforts.get(profile_id)
    provider = (
        None if profile_providers is None or not profile_id else profile_providers.get(profile_id)
    )
    if provider is None and profile_id:
        if profile_id.startswith("codex"):
            provider = "codex"
        elif profile_id.startswith("grok"):
            provider = "grok"
    return format_executor_model(
        resolved_model,
        effort=resolved_effort,
        provider=provider,
        profile_id=profile_id,
    )


def format_executor_model(
    model: str | None,
    *,
    effort: str | None = None,
    provider: str | None = None,
    profile_id: str | None = None,
) -> str:
    """Turn a registry/step model slug into a full dashboard label."""

    slug = (model or "").strip()
    effort_label = (effort or "").strip().lower() or None
    provider_key = (provider or "").strip().lower()
    if not provider_key and profile_id:
        if profile_id.startswith("codex"):
            provider_key = "codex"
        elif profile_id.startswith("grok"):
            provider_key = "grok"

    if not slug and not profile_id:
        return "ещё не назначен"

    base: str
    is_codex = provider_key == "codex" or (
        not provider_key
        and (slug.lower() in {"codex", "auto"} or (profile_id or "").startswith("codex"))
    )
    if is_codex:
        variant = _codex_variant_label(slug)
        base = f"Codex {variant}".strip() if variant else "Codex"
    elif slug.lower() in {"grok-build", "grok"} or provider_key == "grok":
        if slug.lower() == "grok-build" or (not slug and provider_key == "grok"):
            base = "Grok Build"
        elif slug.lower() == "grok":
            base = "Grok"
        else:
            base = _humanize_model_slug(slug) if slug else "Grok"
    elif slug:
        base = _humanize_model_slug(slug)
    elif profile_id:
        base = profile_id
    else:
        base = "ещё не назначен"

    if effort_label:
        return f"{base} · {effort_label}"
    return base


def _codex_variant_label(slug: str) -> str | None:
    """Map Codex model slugs to short product names (Sol / Terra / Luna / …)."""

    lowered = slug.strip().lower()
    if not lowered or lowered in {"codex", "auto"}:
        return None
    # gpt-5.6-sol → Sol; gpt-5.6-terra → Terra; keep full human form otherwise.
    if lowered.endswith("-sol") or lowered == "sol":
        return "Sol"
    if lowered.endswith("-terra") or lowered == "terra":
        return "Terra"
    if lowered.endswith("-luna") or lowered == "luna":
        return "Luna"
    if lowered.startswith("gpt-"):
        return _humanize_model_slug(slug)
    return _humanize_model_slug(slug)


def _humanize_model_slug(slug: str) -> str:
    """Best-effort prettify of model ids (gpt-5.6-sol → GPT-5.6 Sol)."""

    text = slug.strip().replace("_", "-")
    if not text:
        return text
    # Drop trailing calendar versions like -2025-08-07 from display names.
    text = re.sub(r"-\d{4}-\d{2}-\d{2}$", "", text)
    parts = text.split("-")
    pretty: list[str] = []
    index = 0
    while index < len(parts):
        part = parts[index]
        lower = part.lower()
        if lower == "gpt":
            version_parts: list[str] = []
            look = index + 1
            # Take at most one numeric version token (5 / 5.6 / 5.1).
            if look < len(parts) and re.fullmatch(r"\d+(?:\.\d+)*", parts[look]):
                version_parts.append(parts[look])
                look += 1
            pretty.append("GPT-" + version_parts[0] if version_parts else "GPT")
            index = look
            continue
        if lower in {
            "sol",
            "terra",
            "luna",
            "nano",
            "mini",
            "build",
            "composer",
            "codex",
            "grok",
        }:
            pretty.append(part.capitalize())
        elif re.fullmatch(r"\d+(?:\.\d+)*", part):
            pretty.append(part)
        else:
            pretty.append(part.capitalize() if part.islower() else part)
        index += 1
    return " ".join(pretty)


def dashboard_revision_for(
    tasks: Sequence[Task],
    model_by_task: Mapping[uuid.UUID, str],
    *,
    limit_fingerprints: Sequence[str] = (),
) -> int:
    """Stable content identity for the dashboard; equal content must not re-edit."""

    parts = [
        f"{task.id}:{task.version}:{task.status.value}:{model_by_task.get(task.id, '')}"
        for task in tasks
    ]
    parts.extend(limit_fingerprints)
    digest = hashlib.sha256("\n".join(parts).encode()).hexdigest()
    return int(digest[:8], 16) % (2**31 - 1) or 1


async def build_project_status_dashboard(
    session: AsyncSession,
    chat_id: int,
    *,
    project_names: Mapping[str, str] | None = None,
    profile_models: Mapping[str, str] | None = None,
    profile_efforts: Mapping[str, str | None] | None = None,
    profile_providers: Mapping[str, str] | None = None,
    subscription_profiles: Sequence[ProviderProfileConfig] | None = None,
    subscription_snapshots: Sequence[SubscriptionLimitSnapshot] | None = None,
) -> DashboardCard:
    """Build the single in-progress task list plus subscription limits."""

    tasks = list(
        (
            await session.scalars(
                select(Task)
                .where(
                    Task.source_chat_id == chat_id,
                    Task.status.not_in(USER_TERMINAL_TASK_STATUSES),
                    Task.task_type != "discussion_agent_internal",
                )
                .order_by(Task.created_at.asc(), Task.id.asc())
            )
        ).all()
    )
    model_by_task: dict[uuid.UUID, str] = {}
    lines = [f"<b>{telegram_html(DASHBOARD_CARD_TITLE)}</b>", ""]
    if not tasks:
        lines.append("Сейчас нет активных задач.")
    else:
        for task in tasks:
            profile_id = await _active_executor_profile(session, task.id)
            step_model = await _latest_step_model(session, task.id)
            model = model_label_for_profile(
                profile_id,
                profile_models=profile_models,
                profile_efforts=profile_efforts,
                profile_providers=profile_providers,
                model=step_model,
            )
            # Before an executor step is claimed, surface the durable project /model pin.
            if model == "ещё не назначен" and task.project_id is not None:
                preference = await load_preference(session, task.project_id)
                if not preference.is_auto:
                    model = f"{format_preference_label(preference)} (по умолчанию для проекта)"
            model_by_task[task.id] = model
            project_id = task.project_id
            if project_id and project_names is not None and project_id in project_names:
                project_label = project_names[project_id]
            else:
                project_label = project_id or "без проекта"
            lines.append(
                f"• <b>{telegram_html(project_label)}</b> · "
                f"#{telegram_html(task_number_label(task))} · {telegram_html(model)}"
            )
            lines.append(f"  {telegram_html(task_sense_sentence(task))}")

    # Delivery must not open provider state dirs (no auth ACL). Prefer DB snapshots
    # collected by the executor process; optional live collection is test-only.
    if subscription_snapshots is None:
        subscription_snapshots = await load_subscription_limits(session)
    del subscription_profiles  # reserved for tests / offline collectors
    if subscription_snapshots:
        if lines[-1]:
            lines.append("")
        lines.append("<b>Лимиты подписки</b>")
        lines.extend(
            format_subscription_limits_html(subscription_snapshots, html_escape=telegram_html)
        )
        updated_at = max((snap.observed_at for snap in subscription_snapshots), default=None)
        if updated_at is not None:
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=UTC)
            stamp = updated_at.astimezone(UTC).strftime("%d.%m %H:%M UTC")
            lines.append("")
            lines.append(f"<i>Обновлено: {telegram_html(stamp)}</i>")

    fingerprints = tuple(snap.fingerprint() for snap in (subscription_snapshots or ()))
    html_body = "\n".join(lines).rstrip()
    return DashboardCard(
        chat_id=chat_id,
        revision=dashboard_revision_for(tasks, model_by_task, limit_fingerprints=fingerprints),
        html=split_message(html_body)[0],
    )


async def _active_executor_profile(session: AsyncSession, task_id: uuid.UUID) -> str | None:
    run = await session.scalar(
        select(Run).where(Run.task_id == task_id).order_by(Run.created_at.desc()).limit(1)
    )
    if run is None:
        return None
    trusted = run.selected_route.get("trusted_profile_id")
    if isinstance(trusted, str) and trusted:
        return trusted
    steps = list(
        (
            await session.scalars(
                select(Step)
                .where(
                    Step.run_id == run.id,
                    Step.step_type.in_(_ACTIVE_PROVIDER_STEPS),
                    Step.executor_profile_id.is_not(None),
                )
                .order_by(Step.ordinal.desc())
            )
        ).all()
    )
    for step in steps:
        if step.status in {StepStatus.LEASED, StepStatus.RUNNING} and step.executor_profile_id:
            return step.executor_profile_id
    for step in steps:
        if step.executor_profile_id:
            return step.executor_profile_id
    return None


async def _latest_step_model(session: AsyncSession, task_id: uuid.UUID) -> str | None:
    """Prefer the model recorded on the latest provider step result when available."""

    run = await session.scalar(
        select(Run).where(Run.task_id == task_id).order_by(Run.created_at.desc()).limit(1)
    )
    if run is None:
        return None
    steps = list(
        (
            await session.scalars(
                select(Step)
                .where(
                    Step.run_id == run.id,
                    Step.step_type.in_(_ACTIVE_PROVIDER_STEPS),
                    Step.result.is_not(None),
                )
                .order_by(Step.ordinal.desc())
            )
        ).all()
    )
    for step in steps:
        result = step.result
        if not isinstance(result, dict):
            continue
        raw = result.get("model")
        if isinstance(raw, str) and raw.strip() and raw.strip().lower() not in {"codex", "auto"}:
            return raw.strip()
    return None


@dataclass(frozen=True, slots=True)
class HistoryReport:
    task_id: uuid.UUID
    chat_id: int
    thread_id: int
    html: str
    revision: int = 1


async def build_task_history_report(
    session: AsyncSession,
    task_id: uuid.UUID,
    *,
    project_names: Mapping[str, str] | None = None,
    expected_status: TaskStatus | None = None,
) -> HistoryReport | None:
    """Build a one-shot terminal report for the «История» topic.

    Returns ``None`` when the task has no reportable outcome or source chat.
    Approval wait time is excluded from the work duration.
    """

    task = await session.get(Task, task_id)
    if task is None or task.status not in USER_REPORTABLE_TASK_STATUSES:
        return None
    if expected_status is not None and task.status is not expected_status:
        return None
    if not task.source_chat_id:
        return None
    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == task.source_chat_id,
            TopicMapping.topic_kind == HISTORY_TOPIC_KIND.value,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is None:
        return None

    project_id = task.project_id or "без проекта"
    if project_names is not None and task.project_id and task.project_id in project_names:
        project_label = project_names[task.project_id]
    else:
        project_label = project_id

    task_summary = task_sense_sentence(task)
    result_summary = (
        await _history_summary(session, task) if task.status is TaskStatus.COMPLETED else None
    )
    tokens_in, tokens_out, tokens_cached = await _history_token_totals(session, task.id)
    work_seconds = await _history_work_seconds(session, task)
    worker = await _task_worker_label(session, task.id)
    preview_url = await _published_preview_url(session, task.id)

    number = task_number_label(task)
    lines = [
        f"<b>#{telegram_html(number)}</b> · <b>{telegram_html(project_label)}</b>",
        f"<b>Задача:</b> {telegram_html(task_summary)}",
    ]
    if task.status is TaskStatus.COMPLETED:
        lines.extend(
            (
                "<b>Итог:</b> ✅ Завершена успешно",
                f"<b>Результат:</b> {telegram_html(result_summary or 'Без описания')}",
            )
        )
    else:
        stage, reason = await _failure_details(session, task)
        lines.append(f"<b>Итог:</b> {_task_outcome_label(task.status)}")
        if stage:
            lines.append(f"<b>Этап:</b> {step_type_label(stage)}")
        lines.append(f"<b>Причина:</b> {telegram_html(reason)}")
    if worker is not None:
        lines.append(f"<b>Исполнитель:</b> {telegram_html(worker)}")
    if preview_url is not None:
        escaped_url = telegram_html(preview_url)
        lines.append(f'<b>Прототип:</b> <a href="{escaped_url}">Открыть</a>')
    lines.extend(
        (
            "",
            (
                f"Токены: <code>{telegram_html(_format_count(tokens_in))}</code> вх / "
                f"<code>{telegram_html(_format_count(tokens_out))}</code> вых / "
                f"<code>{telegram_html(_format_count(tokens_cached))}</code> кэш"
            ),
            f"Работа: <code>{telegram_html(_format_duration_ru(work_seconds))}</code>",
        )
    )
    return HistoryReport(
        task_id=task.id,
        chat_id=int(task.source_chat_id),
        thread_id=int(mapping.message_thread_id),
        html=split_message("\n".join(lines))[0],
    )


async def enqueue_task_history_report(session: AsyncSession, task_id: uuid.UUID) -> None:
    """Queue a one-shot terminal report into the forum's «История» topic."""

    report = await build_task_history_report(session, task_id)
    if report is None:
        return
    task = await session.get(Task, task_id)
    assert task is not None
    key = f"telegram:{TASK_HISTORY_ROLE}:task:{task_id}:outcome:{task.status.value}"
    existing = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == "telegram",
            TransactionalOutbox.idempotency_key == key,
        )
    )
    if existing is not None:
        return
    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == report.chat_id,
            TopicMapping.topic_kind == HISTORY_TOPIC_KIND.value,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is None:
        return
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="task",
            linked_entity_id=task_id,
            idempotency_key=key,
            payload={
                "role": TASK_HISTORY_ROLE,
                "chat_id": report.chat_id,
                "message_thread_id": report.thread_id,
                "topic_kind": HISTORY_TOPIC_KIND.value,
                "task_id": str(task_id),
                "terminal_status": task.status.value,
                "revision": report.revision,
            },
        )
    )


async def _history_summary(session: AsyncSession, task: Task) -> str:
    """Prefer the approval human summary, then execute result text, then task sense."""

    approval = await session.scalar(
        select(Approval)
        .join(Step, Approval.step_id == Step.id)
        .join(Run, Step.run_id == Run.id)
        .where(
            Run.task_id == task.id,
            Approval.status.in_(
                {ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED, ApprovalStatus.PENDING}
            ),
        )
        .order_by(Approval.requested_at.desc())
        .limit(1)
    )
    if approval is not None and approval.human_summary and approval.human_summary.strip():
        return _one_line_summary(approval.human_summary)

    run = await session.scalar(
        select(Run).where(Run.task_id == task.id).order_by(Run.created_at.desc()).limit(1)
    )
    if run is not None:
        steps = list(
            (
                await session.scalars(
                    select(Step)
                    .where(
                        Step.run_id == run.id,
                        Step.step_type.in_(_ACTIVE_PROVIDER_STEPS),
                        Step.result.is_not(None),
                    )
                    .order_by(Step.ordinal.desc())
                )
            ).all()
        )
        for step in steps:
            result = step.result if isinstance(step.result, dict) else {}
            for key in ("implementation_summary", "summary", "text"):
                value = result.get(key)
                if isinstance(value, str) and value.strip():
                    return _one_line_summary(value)
    return _one_line_summary(task_sense_sentence(task))


async def _completion_report(
    session: AsyncSession, task: Task
) -> tuple[str, tuple[dict[str, str | None], ...], tuple[str, ...]]:
    """Return bounded detail, provider-reported checks, and trusted gate names."""

    approval = await session.scalar(
        select(Approval)
        .join(Step, Approval.step_id == Step.id)
        .join(Run, Step.run_id == Run.id)
        .where(
            Run.task_id == task.id,
            Approval.status.in_({ApprovalStatus.APPROVED, ApprovalStatus.CONSUMED}),
        )
        .order_by(Approval.requested_at.desc())
        .limit(1)
    )
    if approval is not None and approval.human_summary.strip():
        approval_step = await session.get(Step, approval.step_id)
        gates: list[str] = []
        envelope: dict[str, object] = {}
        if approval_step is not None:
            raw_envelope = approval_step.payload.get("action_envelope")
            envelope = raw_envelope if isinstance(raw_envelope, dict) else {}
            raw_gates = envelope.get("gates")
            if isinstance(raw_gates, list):
                gates = [
                    str(gate.get("name", "check"))
                    for gate in raw_gates
                    if isinstance(gate, dict) and gate.get("name")
                ]
        agent_checks = _envelope_agent_checks(envelope)
        return (
            _concise_completion_report(approval.human_summary),
            agent_checks,
            tuple(gates[:12]),
        )

    run = await session.scalar(
        select(Run).where(Run.task_id == task.id).order_by(Run.created_at.desc()).limit(1)
    )
    if run is not None:
        steps = list(
            (
                await session.scalars(
                    select(Step)
                    .where(Step.run_id == run.id, Step.result.is_not(None))
                    .order_by(Step.ordinal.desc())
                )
            ).all()
        )
        for step in steps:
            result = step.result if isinstance(step.result, dict) else {}
            structured = result.get("structured_output")
            sources = (structured, result) if isinstance(structured, dict) else (result,)
            for source in sources:
                for key in ("implementation_summary", "summary", "text"):
                    value = source.get(key)
                    if isinstance(value, str) and value.strip():
                        return _concise_completion_report(value), (), ()
    return task_sense_sentence(task), (), ()


def _envelope_agent_checks(envelope: Mapping[str, object]) -> tuple[dict[str, str | None], ...]:
    raw = envelope.get("agent_checks")
    if not isinstance(raw, list):
        return ()
    checks: list[dict[str, str | None]] = []
    for item in raw[:12]:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        status = item.get("status")
        detail = item.get("detail")
        if not isinstance(name, str) or not isinstance(status, str):
            continue
        checks.append(
            {
                "name": name,
                "status": status,
                "detail": detail if isinstance(detail, str) else None,
            }
        )
    return tuple(checks)


def _agent_check_html(check: Mapping[str, str | None]) -> str:
    labels = {
        "passed": "заявлено: пройдено",
        "failed": "заявлено: не пройдено",
        "not_run": "не запускалось",
        "unavailable": "недоступно",
    }
    status = check.get("status") or "unavailable"
    line = (
        f"• {telegram_html(check.get('name') or 'проверка')} — "
        f"{telegram_html(labels.get(status, status))}"
    )
    detail = check.get("detail")
    if detail:
        line += f" ({telegram_html(detail)})"
    return line


def _envelope_review_warnings(
    envelope: Mapping[str, object],
) -> tuple[dict[str, str | int | None], ...]:
    review = envelope.get("review_evidence")
    raw = review.get("warnings") if isinstance(review, dict) else None
    if not isinstance(raw, list):
        return ()
    warnings: list[dict[str, str | int | None]] = []
    for item in raw[:12]:
        if not isinstance(item, dict):
            continue
        summary = item.get("summary")
        if not isinstance(summary, str):
            continue
        warnings.append(
            {
                "classification": (
                    item.get("classification")
                    if isinstance(item.get("classification"), str)
                    else None
                ),
                "summary": summary,
                "path": item.get("path") if isinstance(item.get("path"), str) else None,
                "line": item.get("line") if isinstance(item.get("line"), int) else None,
            }
        )
    return tuple(warnings)


def _review_warning_html(warning: Mapping[str, str | int | None]) -> str:
    path = warning.get("path")
    location = f"{path}:{warning['line']}" if path and warning.get("line") else path
    prefix = f"<code>{telegram_html(location)}</code> — " if location else ""
    return f"⚠️ {prefix}{telegram_html(warning.get('summary') or 'Review warning')}"


def _approval_fact_lines(
    envelope: Mapping[str, object],
    human_summary: str,
) -> list[str]:
    """One canonical fact block shared by project and global approval cards."""

    display_summary = _approval_display_summary(human_summary)
    lines = ["<b>Что изменится</b>", f"• {telegram_html(display_summary)}"]
    raw_paths = envelope.get("changed_files")
    paths = (
        [str(path) for path in raw_paths if isinstance(path, str)]
        if isinstance(raw_paths, list)
        else []
    )
    if paths:
        visible = ", ".join(paths[:6])
        suffix = f" и ещё {len(paths) - 6}" if len(paths) > 6 else ""
        lines.append(f"• Файлы: <code>{telegram_html(visible)}</code>{suffix}")
    review_warnings = tuple(
        warning for warning in _envelope_review_warnings(envelope) if warning.get("path")
    )
    if review_warnings:
        lines.extend(("", "<b>Предупреждения ревью</b>"))
        lines.extend(_review_warning_html(warning) for warning in review_warnings)
    lines.extend(("", "<b>Проверено Vuzol</b>"))
    raw_gates = envelope.get("gates")
    gates = raw_gates if isinstance(raw_gates, list) else []
    for gate in gates:
        if not isinstance(gate, dict):
            continue
        duration = int(gate.get("duration_ms", 0)) / 1000
        lines.append(
            f"✅ {telegram_html(gate.get('name', 'проверка'))} — пройдено ({duration:.1f} с)"
        )
    lines.extend(("", "Применить этот результат локально?"))
    return lines


def _approval_display_summary(summary: str) -> str:
    """Keep legacy provider transcripts out of approval projections."""

    cleaned = " ".join(summary.split()).strip()
    looks_like_transcript = (
        len(cleaned) > 600
        or cleaned.casefold().count("i'll ") > 1
        or cleaned.casefold().count("next i'll") > 0
    )
    if looks_like_transcript:
        return "Изменения подготовлены и прошли настроенные проверки."
    return cleaned.lstrip("#*- ").strip() or "Изменения подготовлены."


def _approval_status_label(status: ApprovalStatus) -> str:
    return {
        ApprovalStatus.PENDING: "Ждёт решения",
        ApprovalStatus.APPROVED: "Принято",
        ApprovalStatus.CONSUMED: "Принято",
        ApprovalStatus.REJECTED: "Отклонено",
        ApprovalStatus.EXPIRED: "Истекло",
    }[status]


async def _failure_details(session: AsyncSession, task: Task) -> tuple[str | None, str]:
    """Find the exact failed/blocked stage and its safest available reason."""

    run = await session.scalar(
        select(Run).where(Run.task_id == task.id).order_by(Run.created_at.desc()).limit(1)
    )
    if run is not None:
        step = await session.scalar(
            select(Step)
            .where(
                Step.run_id == run.id,
                Step.status.in_({StepStatus.FAILED, StepStatus.BLOCKED}),
            )
            .order_by(Step.ordinal.desc())
            .limit(1)
        )
        category = step.failure_category if step is not None else run.failure_category
        summary = step.failure_summary if step is not None else run.failure_summary
        reason = summary or category
        if reason:
            return (step.step_type if step is not None else None), _bounded_report(reason, 700)

    event = await session.scalar(
        select(Event)
        .where(Event.entity_type == "task", Event.entity_id == task.id)
        .order_by(Event.created_at.desc())
        .limit(1)
    )
    payload = event.payload if event is not None and isinstance(event.payload, dict) else {}
    reason = payload.get("reason") or payload.get("summary") or payload.get("category")
    return None, _bounded_report(str(reason or "Причина не указана"), 700)


def _task_outcome_label(status: TaskStatus) -> str:
    if status is TaskStatus.COMPLETED:
        return "✅ Завершена успешно"
    if status is TaskStatus.BLOCKED:
        return "⛔ Завершена неудачно (заблокирована)"
    return "❌ Завершена неудачно"


def _bounded_report(text: str, limit: int = 1_800) -> str:
    cleaned = "\n".join(line.strip() for line in text.strip().splitlines() if line.strip())
    if len(cleaned) > limit:
        return cleaned[: limit - 1].rstrip() + "…"
    return cleaned or "Без описания"


def _concise_completion_report(text: str, *, limit: int = 900, bullet_limit: int = 6) -> str:
    """Keep implementation facts while dropping provider hand-off and suggestion sections."""

    lines = [line.strip() for line in text.strip().splitlines()]
    first_index = next((index for index, line in enumerate(lines) if line), None)
    if first_index is None:
        return "Без описания"
    first = _plain_report_text(lines[first_index])
    output = [first] if first else []
    collecting_details = False
    bullets = 0
    for raw in lines[first_index + 1 :]:
        if not raw:
            continue
        plain = _plain_report_text(raw)
        heading = plain.rstrip(":").casefold()
        if heading.startswith(
            (
                "план",
                "для реализации",
                "основные точки",
                "файлы",
                "запуск",
                "подключение",
                "следующ",
                "plan",
                "how to run",
                "next step",
            )
        ):
            break
        if heading in {"реализовано", "что сделано", "готово", "implemented", "completed"}:
            collecting_details = True
            continue
        if raw.lstrip().startswith(("- ", "* ")) and collecting_details:
            if bullets >= bullet_limit:
                break
            output.append(f"• {plain.lstrip('-* ').strip()}")
            bullets += 1
    return _bounded_report("\n".join(output), limit)


def _plain_report_text(text: str) -> str:
    value = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text.strip())
    return value.lstrip("# ").strip()


async def _task_worker_label(session: AsyncSession, task_id: uuid.UUID) -> str | None:
    """Return the actual execution worker model, excluding planners and reviewers."""

    step = await session.scalar(
        select(Step)
        .join(Run, Step.run_id == Run.id)
        .where(
            Run.task_id == task_id,
            Step.step_type.in_(_WORKER_STEP_TYPES),
            Step.executor_profile_id.is_not(None),
        )
        .order_by(Run.created_at.desc(), Step.ordinal.desc())
        .limit(1)
    )
    if step is None:
        return None
    profile_id = getattr(step, "executor_profile_id", None)
    if not isinstance(profile_id, str) or not profile_id:
        return None
    raw_result = getattr(step, "result", None)
    result = raw_result if isinstance(raw_result, dict) else {}
    model = result.get("model") if isinstance(result.get("model"), str) else None
    return format_executor_model(
        model,
        profile_id=profile_id,
    )


async def _history_token_totals(session: AsyncSession, task_id: uuid.UUID) -> tuple[int, int, int]:
    rows = list(
        (await session.scalars(select(UsageRecord).where(UsageRecord.task_id == task_id))).all()
    )
    tokens_in = sum(int(row.input_tokens or 0) for row in rows)
    tokens_out = sum(int(row.output_tokens or 0) for row in rows)
    tokens_cached = sum(int(row.cached_tokens or 0) for row in rows)
    return tokens_in, tokens_out, tokens_cached


async def _history_work_seconds(session: AsyncSession, task: Task) -> int:
    """Active work time excluding human approval wait.

    Prefer the sum of provider invocation durations (never includes approval wait).
    If no usage rows exist, fall back to wall-clock minus approval pending spans.
    """

    rows = list(
        (await session.scalars(select(UsageRecord).where(UsageRecord.task_id == task.id))).all()
    )
    if rows:
        total_ms = sum(max(0, int(row.duration_ms or 0)) for row in rows)
        return max(0, total_ms // 1000)

    state = sqlalchemy_inspect(task, raiseerr=False)
    if state is not None and {"created_at", "updated_at"} & state.expired_attributes:
        await session.refresh(task, attribute_names=("created_at", "updated_at"))
    started = task.created_at
    ended = task.updated_at or datetime.now(UTC)
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    if ended.tzinfo is None:
        ended = ended.replace(tzinfo=UTC)
    total = max(0.0, (ended - started).total_seconds())

    approvals = list(
        (
            await session.scalars(
                select(Approval)
                .join(Step, Approval.step_id == Step.id)
                .join(Run, Step.run_id == Run.id)
                .where(Run.task_id == task.id)
            )
        ).all()
    )
    for approval in approvals:
        requested = approval.requested_at
        decided = approval.decided_at
        if requested is None or decided is None:
            continue
        if requested.tzinfo is None:
            requested = requested.replace(tzinfo=UTC)
        if decided.tzinfo is None:
            decided = decided.replace(tzinfo=UTC)
        total -= max(0.0, (decided - requested).total_seconds())
    return max(0, int(total))


def _one_line_summary(text: str, *, limit: int = 280) -> str:
    cleaned = " ".join(text.split()).strip()
    if not cleaned:
        return "No description"
    for separator in (". ", "! ", "? ", "\n"):
        if separator in cleaned:
            cleaned = cleaned.split(separator, 1)[0].strip()
            break
    cleaned = cleaned.rstrip(".!?").strip()
    if len(cleaned) > limit:
        cleaned = cleaned[: limit - 1].rstrip() + "…"
    return cleaned or "No description"


def _format_count(value: int) -> str:
    return f"{max(0, int(value)):,}"


def _format_duration(seconds: int) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _format_duration_ru(seconds: int) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours} ч {minutes} мин"
    if minutes:
        return f"{minutes} мин {secs} с"
    return f"{secs} с"


async def enqueue_project_status_dashboard(session: AsyncSession, chat_id: int) -> None:
    """Queue a refresh of the existing «Статус проектов» topic (kind=task_dashboard).

    Product policy always targets :data:`STATUS_DASHBOARD_TOPIC_KIND`. The stable
    thread id comes from the forum's configured mapping — never from a display name
    and never from a hard-coded chat. No new Telegram topic is created.
    """

    mapping = await session.scalar(
        select(TopicMapping).where(
            TopicMapping.chat_id == chat_id,
            TopicMapping.topic_kind == STATUS_DASHBOARD_TOPIC_KIND.value,
            TopicMapping.enabled.is_(True),
        )
    )
    if mapping is None:
        return
    card = await build_project_status_dashboard(session, chat_id)
    key = f"telegram:{PROJECT_STATUS_DASHBOARD_ROLE}:{chat_id}:revision:{card.revision}"
    existing = await session.scalar(
        select(TransactionalOutbox.id).where(
            TransactionalOutbox.destination == "telegram",
            TransactionalOutbox.idempotency_key == key,
        )
    )
    if existing is not None:
        return
    # Same transaction can call this twice (e.g. intake_ack + approval_card). The DB
    # SELECT above does not see unflushed pending rows, so also scan session.new.
    for pending in session.new:
        if (
            isinstance(pending, TransactionalOutbox)
            and pending.destination == "telegram"
            and pending.idempotency_key == key
        ):
            return
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="topic_mapping",
            linked_entity_id=mapping.id,
            idempotency_key=key,
            payload={
                "role": PROJECT_STATUS_DASHBOARD_ROLE,
                "chat_id": chat_id,
                "message_thread_id": mapping.message_thread_id,
                "topic_kind": STATUS_DASHBOARD_TOPIC_KIND.value,
                "revision": card.revision,
            },
        )
    )


async def enqueue_task_status_projection(
    session: AsyncSession,
    task: Task,
    run: Run | None = None,
    *,
    role: str | None = None,
) -> None:
    """Refresh a task's project-topic card and the global active dashboard."""

    if not task.source_chat_id:
        return
    chat_id = int(task.source_chat_id)
    if task.source_thread_id is None:
        await enqueue_project_status_dashboard(session, chat_id)
        return
    intake = await session.scalar(
        select(TelegramIntakeMessage)
        .where(TelegramIntakeMessage.task_id == task.id)
        .order_by(TelegramIntakeMessage.created_at.desc())
        .limit(1)
    )
    if role is None:
        # Approval is a stage of the same project-topic task/result card, not a
        # second message in a remote approvals topic.
        role = "intake_ack"
    payload: dict[str, object] = {
        "chat_id": task.source_chat_id,
        "message_thread_id": task.source_thread_id,
        "role": role,
        "task_id": str(task.id),
    }
    if run is not None:
        payload["run_id"] = str(run.id)
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="telegram_intake" if intake is not None else "task",
            linked_entity_id=intake.id if intake is not None else task.id,
            idempotency_key=f"telegram:{role}:task:{task.id}:revision:{task.version}",
            payload=payload,
        )
    )
    materialization = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    if materialization is not None:
        session.add(
            TransactionalOutbox(
                destination="work_package_projection",
                operation_type="render_status",
                linked_entity_type="work_package",
                linked_entity_id=materialization.work_package_id,
                idempotency_key=(f"wp:projection:task-status:{task.id}:{task.version}:{role}"),
                payload={"package_id": str(materialization.work_package_id)},
            )
        )
        if task.status is TaskStatus.WAITING_APPROVAL:
            session.add(
                TransactionalOutbox(
                    destination="work_package_projection",
                    operation_type="render_action",
                    linked_entity_type="work_package",
                    linked_entity_id=materialization.work_package_id,
                    idempotency_key=(
                        f"wp:projection:package-result:{task.id}:{task.version}"
                    ),
                    payload={"package_id": str(materialization.work_package_id)},
                )
            )
    await enqueue_project_status_dashboard(session, chat_id)


async def enqueue_terminal_task_projections(
    session: AsyncSession, task: Task, run: Run | None = None
) -> None:
    """Publish every terminal side effect, including package sequencing."""

    await enqueue_task_status_projection(session, task, run)
    await enqueue_task_history_report(session, task.id)
    link = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    if link is None:
        return
    session.add(
        TransactionalOutbox(
            destination="work_package_sequence",
            operation_type="observe_task_terminal",
            linked_entity_type="task",
            linked_entity_id=task.id,
            idempotency_key=f"work-package:terminal:{task.id}:{task.version}",
            payload={
                "work_package_id": str(link.work_package_id),
                "ordinal": link.ordinal,
                "task_status": task.status.value,
            },
        )
    )


async def build_status_card(session: AsyncSession, task_id: uuid.UUID) -> StatusCard:
    """Build presentation solely from canonical database state."""

    task = await session.get(Task, task_id)
    if task is None:
        raise LookupError(f"task not found: {task_id}")
    run = await session.scalar(
        select(Run).where(Run.task_id == task_id).order_by(Run.created_at.desc()).limit(1)
    )
    step = None
    if run is not None:
        step = await session.scalar(
            select(Step)
            .where(
                Step.run_id == run.id,
                Step.status.notin_(
                    (StepStatus.PENDING, StepStatus.COMPLETED, StepStatus.CANCELLED)
                ),
            )
            .order_by(Step.ordinal.desc())
            .limit(1)
        )
        if step is None:
            step = await session.scalar(
                select(Step)
                .where(Step.run_id == run.id)
                .order_by(Step.ordinal.desc())
                .limit(1)
            )
    redo_requested = await session.scalar(
        select(Event.id)
        .where(
            Event.entity_type == "task",
            Event.entity_id == task_id,
            Event.event_type == "result.redo_requested",
        )
        .limit(1)
    )
    materialization = await session.scalar(
        select(MaterializationLink).where(MaterializationLink.task_id == task.id)
    )
    package = None
    revision = None
    plan_size = None
    if materialization is not None:
        package = await session.get(WorkPackage, materialization.work_package_id)
        revision = await session.get(PlanRevision, materialization.plan_revision_id)
        plan_size = await session.scalar(
            select(func.count(PlanRevisionItem.id)).where(
                PlanRevisionItem.plan_revision_id == materialization.plan_revision_id
            )
        )
    title = task_title(task)
    scope = task.project_id or "личный"
    final_package_result = bool(
        materialization is not None
        and plan_size is not None
        and materialization.ordinal == int(plan_size)
        and task.status is TaskStatus.WAITING_APPROVAL
    )
    final_package_delivery_failure = bool(
        materialization is not None
        and plan_size is not None
        and materialization.ordinal == int(plan_size)
        and step is not None
        and step.step_type == "approval"
        and task.status in {TaskStatus.BLOCKED, TaskStatus.FAILED}
    )
    lines = [f"<b>{telegram_html(title)}</b>"]
    if materialization is not None and plan_size:
        lines.append(f"Пункт плана: <b>{materialization.ordinal}/{int(plan_size)}</b>")
    draft = task.task_draft if isinstance(task.task_draft, dict) else {}
    if any(draft.get(key) for key in ("task_summary", "normalized_title", "goal", "title")):
        lines.append(f"Задача: {telegram_html(task_sense_sentence(task))}")
    status_label = (
        "Работа завершена"
        if final_package_result or final_package_delivery_failure
        else
        _task_outcome_label(task.status)
        if task.status in USER_REPORTABLE_TASK_STATUSES
        else user_status_label(task.status)
    )
    lines.extend(
        (
            f"Проект: {telegram_html(scope)}",
            f"Статус: <b>{status_label}</b>",
        )
    )
    if step is not None and not final_package_result and not final_package_delivery_failure:
        lines.append(f"Этап: {step_type_label(step.step_type)} ({step_status_label(step.status)})")
    worker = await _task_worker_label(session, task.id) if run is not None else None
    if worker is not None:
        lines.append(f"Исполнитель: {telegram_html(worker)}")
    approval = None
    if step is not None and step.status.value == "waiting_approval":
        approval = await session.scalar(
            select(Approval).where(
                Approval.step_id == step.id,
                Approval.status == ApprovalStatus.PENDING,
            )
        )
    if (
        approval is not None
        and materialization is not None
        and plan_size is not None
        and materialization.ordinal == int(plan_size)
    ):
        # The final item has finished its own work. Its approval belongs to the
        # whole accepted package and is rendered on a separate package-result
        # card, not on the item card.
        approval = None
        lines.append("Результат пункта готов; итог плана ожидает вашего решения.")
    if run is not None and run.selected_route:
        executor = (
            run.selected_route.get("trusted_profile_id")
            or run.selected_route.get("executor")
            or run.selected_route.get("profile_id")
        )
        if executor and worker is None:
            lines.append(f"Маршрут: {telegram_html(executor)}")
        worktree = await session.scalar(select(Worktree).where(Worktree.run_id == run.id))
        if worktree is not None and worktree.result_commit and approval is None:
            lines.append(f"Доставка: {delivery_state_label(worktree.delivery_state)}")
        usage = (
            await session.execute(
                select(
                    func.coalesce(func.sum(UsageRecord.input_tokens), 0),
                    func.coalesce(func.sum(UsageRecord.output_tokens), 0),
                    func.coalesce(func.sum(UsageRecord.cached_tokens), 0),
                ).where(UsageRecord.task_id == task.id)
            )
        ).one()
        input_tokens, output_tokens, cached_tokens = map(int, usage)
        if input_tokens or output_tokens or cached_tokens:
            lines.append(
                f"Токены: {telegram_html(input_tokens)} вх / "
                f"{telegram_html(output_tokens)} вых / "
                f"{telegram_html(cached_tokens)} кэш"
            )
    if task.status is TaskStatus.COMPLETED:
        report, agent_checks, gates = await _completion_report(session, task)
        lines.extend(("", "<b>Отчёт о выполнении</b>", telegram_html(report)))
        preview_url = await _published_preview_url(session, task.id)
        if preview_url is not None:
            escaped_url = telegram_html(preview_url)
            lines.append(f'<b>Прототип:</b> <a href="{escaped_url}">{escaped_url}</a>')
        if agent_checks:
            lines.extend(("", "<b>Проверки агента (не доверенные)</b>"))
            lines.extend(_agent_check_html(check) for check in agent_checks)
        if gates:
            lines.extend(("", "<b>Проверки Vuzol (доверенные)</b>"))
            lines.extend(f"✅ {telegram_html(gate)}" for gate in gates)
    elif (
        task.status in {TaskStatus.FAILED, TaskStatus.BLOCKED}
        and not final_package_delivery_failure
    ):
        failed_stage, reason = await _failure_details(session, task)
        lines.extend(("", "<b>Отчёт о завершении</b>"))
        if failed_stage:
            lines.append(f"<b>Этап:</b> {step_type_label(failed_stage)}")
        lines.append(f"<b>Причина:</b> {telegram_html(reason)}")
    elapsed = max(0, int((datetime.now(UTC) - task.created_at).total_seconds()))
    lines.append(f"Прошло: {_format_duration_ru(elapsed)}")
    identity_footer = _task_identity_footer(task)
    if identity_footer is not None:
        lines.append(identity_footer)
    if redo_requested is not None:
        lines.append(
            "Чтобы переделать результат, отправьте новую задачу отдельным сообщением "
            "и укажите, что именно нужно исправить."
        )
    buttons: tuple[str, ...]
    if final_package_delivery_failure:
        buttons = ()
    elif approval is not None and step is not None:
        envelope = verified_envelope(step, approval)
        lines.extend(("", *_approval_fact_lines(envelope, approval.human_summary)))
        buttons = ("approve", "redo", "reject")
    else:
        buttons = (
            ("start",)
            if run is not None and run.status.value == "created"
            else tuple(status_buttons(task.status.value))
        )
    callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = ()
    if (
        package is not None
        and revision is not None
        and approval is None
        and not final_package_delivery_failure
    ):
        from vuzol.telegram.work_packages import (
            WorkPackageCallback,
            WorkPackageCallbackKind,
            encode_work_package_callback,
        )

        def package_callback(kind: WorkPackageCallbackKind) -> str:
            return encode_work_package_callback(
                WorkPackageCallback(
                    kind=kind,
                    package_id=package.id,
                    revision_number=revision.revision_number,
                    h8=revision.content_hash[:8],
                )
            )

        package_actions: list[tuple[str, str]] = [
            ("Изменить", package_callback(WorkPackageCallbackKind.CONTINUE_DISCUSSION))
        ]
        if task.status in {TaskStatus.FAILED, TaskStatus.BLOCKED, TaskStatus.QUOTA_EXHAUSTED}:
            package_actions.insert(
                0, ("Повторить", package_callback(WorkPackageCallbackKind.RETRY_ITEM))
            )
        callback_buttons = (tuple(package_actions),)
    projection_revision = (
        task.version
        if package is None
        else task.version * 10_000 + package.version
    )
    return StatusCard(
        task_id=task.id,
        revision=projection_revision,
        html="\n".join(lines),
        buttons=buttons,
        approval_id=approval.id if approval is not None else None,
        callback_buttons=callback_buttons,
        work_package_id=None if package is None else package.id,
        plan_revision_id=None if revision is None else revision.id,
        control_status_generation=None if package is None else package.version,
    )


async def _published_preview_url(session: AsyncSession, task_id: uuid.UUID) -> str | None:
    step = await session.scalar(
        select(Step)
        .join(Run, Step.run_id == Run.id)
        .where(
            Run.task_id == task_id,
            Step.step_type == "publish_static",
            Step.status == StepStatus.COMPLETED,
            Step.result.is_not(None),
        )
        .order_by(Run.created_at.desc(), Step.ordinal.desc())
        .limit(1)
    )
    raw_result = getattr(step, "result", None)
    result = raw_result if isinstance(raw_result, dict) else {}
    url = result.get("public_url")
    return url if result.get("status") == "published" and isinstance(url, str) else None


async def build_approval_card(session: AsyncSession, task_id: uuid.UUID) -> StatusCard:
    """Build the global approval projection for the latest exact result."""

    task = await session.get(Task, task_id)
    if task is None:
        raise LookupError(f"task not found: {task_id}")
    approval = await session.scalar(
        select(Approval)
        .join(Step, Approval.step_id == Step.id)
        .join(Run, Step.run_id == Run.id)
        .where(Run.task_id == task_id)
        .order_by(Approval.requested_at.desc())
        .limit(1)
    )
    if approval is None:
        raise LookupError(f"approval not found for task: {task_id}")
    step = await session.get(Step, approval.step_id)
    assert step is not None
    envelope = verified_envelope(step, approval)
    title = task_title(task)
    lines = [
        f"<b>{telegram_html(task.project_id or 'личный')} · {telegram_html(title)}</b>",
    ]
    identity_footer = _task_identity_footer(task)
    if identity_footer is not None:
        lines.append(identity_footer)
    buttons: tuple[str, ...]
    if approval.status is ApprovalStatus.PENDING:
        lines.extend(("", *_approval_fact_lines(envelope, approval.human_summary)))
        buttons = ("approve", "redo", "reject")
    else:
        lines.extend(
            (
                "",
                *_approval_fact_lines(envelope, approval.human_summary)[:-2],
                "",
                f"Решение: <b>{_approval_status_label(approval.status)}</b>",
            )
        )
        buttons = ()
    return StatusCard(
        task_id=task.id,
        revision=task.version,
        html="\n".join(lines),
        buttons=buttons,
        approval_id=approval.id,
    )


class TelegramClient(Protocol):
    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> int: ...

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None: ...

    async def delete_message(self, *, chat_id: int, message_id: int) -> None: ...

    async def pin_message(self, *, chat_id: int, message_id: int) -> bool: ...


class LostTelegramResponse(RuntimeError):
    """Telegram may have accepted a send, but no message ID was received."""


@dataclass(slots=True)
class FakeTelegramClient:
    fail: Exception | None = None
    next_message_id: int = 1
    sent: list[tuple[int, int | None, str]] = field(default_factory=list, init=False)
    edited: list[tuple[int, int, str]] = field(default_factory=list, init=False)
    deleted: list[tuple[int, int]] = field(default_factory=list, init=False)
    pinned: list[tuple[int, int]] = field(default_factory=list, init=False)
    sent_keyboards: list[tuple[tuple[tuple[str, str], ...], ...]] = field(
        default_factory=list, init=False
    )

    async def send_message(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> int:
        del buttons, task_id, approval_id
        if self.fail:
            raise self.fail
        self.sent.append((chat_id, thread_id, html))
        self.sent_keyboards.append(callback_buttons)
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    async def edit_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        html: str,
        buttons: tuple[str, ...] = (),
        task_id: uuid.UUID | None = None,
        approval_id: uuid.UUID | None = None,
        callback_buttons: tuple[tuple[tuple[str, str], ...], ...] = (),
    ) -> None:
        del buttons, task_id, approval_id, callback_buttons
        if self.fail:
            raise self.fail
        self.edited.append((chat_id, message_id, html))

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        if self.fail:
            raise self.fail
        self.deleted.append((chat_id, message_id))

    async def pin_message(self, *, chat_id: int, message_id: int) -> bool:
        self.pinned.append((chat_id, message_id))
        return True


async def apply_status_projection(
    session: AsyncSession,
    client: TelegramClient,
    *,
    card: StatusCard,
    chat_id: int,
    thread_id: int | None,
) -> bool:
    """Apply only a newer desired revision; persist link after a confirmed send."""

    link = await session.scalar(
        select(TelegramMessageLink)
        .where(
            TelegramMessageLink.task_id == card.task_id,
            TelegramMessageLink.message_role == "task_status",
        )
        .with_for_update()
    )
    if link is not None and card.revision <= link.projection_revision:
        return False
    if link is None:
        message_id = await client.send_message(
            chat_id=chat_id,
            thread_id=thread_id,
            html=card.html,
            buttons=card.buttons,
            task_id=card.task_id,
            approval_id=card.approval_id,
        )
        session.add(
            TelegramMessageLink(
                chat_id=chat_id,
                message_thread_id=thread_id,
                message_id=message_id,
                task_id=card.task_id,
                message_role="task_status",
                projection_revision=card.revision,
            )
        )
    else:
        await client.edit_message(
            chat_id=chat_id,
            message_id=link.message_id,
            html=card.html,
            buttons=card.buttons,
            task_id=card.task_id,
            approval_id=card.approval_id,
        )
        link.projection_revision = card.revision
    await session.flush()
    return True


class EditRateLimiter:
    """Per-projection gate; callers naturally coalesce to the latest desired card."""

    def __init__(self, minimum_interval_seconds: float) -> None:
        self._interval = timedelta(seconds=minimum_interval_seconds)
        self._next: dict[uuid.UUID, datetime] = {}
        self._lock = asyncio.Lock()

    async def reserve(self, task_id: uuid.UUID, now: datetime) -> datetime:
        async with self._lock:
            available = max(now, self._next.get(task_id, now))
            self._next[task_id] = available + self._interval
            return available


def status_buttons(status: str) -> Sequence[str]:
    if status == "paused":
        return ("resume", "cancel")
    if status in {
        "received",
        "context_prepared",
        "planned",
        "waiting_approval",
        "executing",
        "validating",
        "reviewing",
        "retrying",
    }:
        return ("pause", "cancel")
    return ()
