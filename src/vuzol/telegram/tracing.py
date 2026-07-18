"""Bounded, durable Telegram diagnostics for small-model orchestration stages."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.interpretation.domain import InterpretationResult
from vuzol.storage.models import (
    Interpretation,
    ProviderBudgetReservation,
    Step,
    Task,
    TransactionalOutbox,
    UsageRecord,
)
from vuzol.telegram.projections import task_number_label, telegram_html

ORCHESTRATION_TRACE_ROLE = "orchestration_trace"
INTERPRETER_TRACE_KIND = "interpreter"
PLANNER_TRACE_KIND = "planner"


def enqueue_interpreter_trace(
    session: AsyncSession,
    *,
    task: Task,
    interpretation: Interpretation,
    result: InterpretationResult,
) -> None:
    """Persist a one-shot trace containing the model draft and call measurements."""

    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="interpretation",
            linked_entity_id=interpretation.id,
            idempotency_key=f"telegram:orchestration:{INTERPRETER_TRACE_KIND}:{interpretation.id}",
            payload={
                "role": ORCHESTRATION_TRACE_ROLE,
                "trace_kind": INTERPRETER_TRACE_KIND,
                "task_id": str(task.id),
                "model_task_draft": result.draft.model_dump(mode="json"),
                "input_tokens": result.input_tokens,
                "output_tokens": result.output_tokens,
                "duration_ms": result.duration_ms,
                "repaired": result.repaired,
            },
        )
    )


def enqueue_planner_trace(session: AsyncSession, *, task: Task, step: Step) -> None:
    """Persist one trace for every committed planner-attempt outcome."""

    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type="step",
            linked_entity_id=step.id,
            idempotency_key=(
                f"telegram:orchestration:{PLANNER_TRACE_KIND}:{step.id}:"
                f"{step.attempt_count}:{step.status.value}"
            ),
            payload={
                "role": ORCHESTRATION_TRACE_ROLE,
                "trace_kind": PLANNER_TRACE_KIND,
                "task_id": str(task.id),
                "attempt": step.attempt_count,
            },
        )
    )


def build_interpreter_trace_html(
    task: Task,
    interpretation: Interpretation,
    payload: dict[str, Any],
) -> str:
    raw = payload.get("model_task_draft")
    effective = interpretation.task_draft
    changed = isinstance(raw, dict) and raw != effective
    repaired = payload.get("repaired") is True
    warnings: list[str] = []
    if repaired:
        warnings.append("⚠️ Первичный ответ не прошёл схему; использована repair-попытка.")
    if changed:
        warnings.append("Policy изменила модельный TaskDraft перед запуском workflow.")
    lines = [
        f"<b>🧭 Интерпретатор · #{telegram_html(task_number_label(task))}</b>",
        f"Проект: <code>{telegram_html(task.project_id or '—')}</code>",
        (
            f"Модель: <code>{telegram_html(interpretation.model)}</code> · "
            f"профиль <code>{telegram_html(interpretation.profile_id)}</code>"
        ),
        (
            f"Prompt: <code>{telegram_html(interpretation.prompt_version)}</code> · "
            f"schema <code>{telegram_html(interpretation.schema_version)}</code>"
        ),
        (
            "Usage: "
            f"{_metric(payload.get('input_tokens'))} in / "
            f"{_metric(payload.get('output_tokens'))} out · "
            f"{_duration(payload.get('duration_ms'))}"
        ),
    ]
    lines.extend(warnings)
    lines.extend(
        [
            "",
            "<b>Выход модели — TaskDraft</b>",
            _json_pre(raw, limit=1_550),
        ]
    )
    if changed:
        lines.extend(
            [
                "",
                "<b>После deterministic policy</b>",
                _json_pre(effective, limit=1_050),
            ]
        )
    return "\n".join(lines)


def build_planner_trace_html(
    task: Task,
    step: Step,
    *,
    usage: UsageRecord | None,
    reservation: ProviderBudgetReservation | None,
) -> str:
    result = step.result if isinstance(step.result, dict) else {}
    text = result.get("text")
    plan = text.strip() if isinstance(text, str) else ""
    finish_reason = result.get("finish_reason")
    warnings: list[str] = []
    if finish_reason == "length":
        warnings.append("⚠️ Ответ остановлен по лимиту токенов (<code>finish_reason=length</code>).")
    if not plan and step.status.value == "completed":
        warnings.append("🚨 План пустой, хотя шаг отмечен как completed.")
    if step.failure_category:
        warnings.append(
            "🚨 Ошибка: "
            f"<code>{telegram_html(step.failure_category)}</code> — "
            f"{telegram_html(step.failure_summary or 'без описания')}"
        )
    model = result.get("model") or (usage.model if usage is not None else "—")
    profile = result.get("profile_id") or step.executor_profile_id or "—"
    input_tokens = usage.input_tokens if usage is not None else None
    output_tokens = usage.output_tokens if usage is not None else None
    output_limit = reservation.reserved_output_tokens if reservation is not None else None
    lines = [
        f"<b>📝 Планировщик · #{telegram_html(task_number_label(task))}</b>",
        f"Проект: <code>{telegram_html(task.project_id or '—')}</code>",
        (
            f"Статус: <code>{telegram_html(step.status.value)}</code> · "
            f"попытка {step.attempt_count}/{step.max_attempts}"
        ),
        (
            f"Модель: <code>{telegram_html(model)}</code> · "
            f"профиль <code>{telegram_html(profile)}</code>"
        ),
        (
            f"Usage: {_metric(input_tokens)} in / {_metric(output_tokens)} out · "
            f"лимит {_metric(output_limit)} out"
        ),
        f"Finish reason: <code>{telegram_html(finish_reason or '—')}</code>",
        "Handoff воркеру: <b>не подключён</b> (результат plan не входит в ProviderRequest)",
    ]
    lines.extend(warnings)
    lines.extend(["", "<b>Выход планировщика</b>"])
    if plan:
        lines.append(_text_pre(plan, limit=2_350))
    else:
        lines.append("<i>Пустой вывод</i>")
    structured = result.get("structured_output")
    if structured is not None:
        lines.extend(["", "<b>Structured output</b>", _json_pre(structured, limit=650)])
    return "\n".join(lines)


def _metric(value: object) -> str:
    return f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else "?"


def _duration(value: object) -> str:
    if not isinstance(value, int) or isinstance(value, bool):
        return "? ms"
    return f"{value / 1_000:.1f}s" if value >= 1_000 else f"{value}ms"


def _json_pre(value: object, *, limit: int) -> str:
    rendered = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    return _text_pre(rendered, limit=limit)


def _text_pre(value: str, *, limit: int) -> str:
    normalized = value.strip()
    if len(normalized) > limit:
        normalized = normalized[: limit - 1].rstrip() + "…"
    return f"<pre>{telegram_html(normalized)}</pre>"
