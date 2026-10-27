"""Bounded, durable Telegram diagnostics for small-model orchestration stages."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.interpretation.domain import InterpretationResult
from vuzol.storage.models import (
    Interpretation,
    Task,
    TransactionalOutbox,
)
from vuzol.telegram.projections import task_number_label, telegram_html

ORCHESTRATION_TRACE_ROLE = "orchestration_trace"
INTERPRETER_TRACE_KIND = "interpreter"
_TRACE_SAMPLE_NAMESPACE = b"vuzol-orchestration-trace-v1:"
# Keep both interpreter drafts readable in Telegram.  These are presentation
# bounds only; the persisted model output and the validated draft are complete.
_INTERPRETER_RAW_JSON_LIMIT = 1_800
_INTERPRETER_POLICY_JSON_LIMIT = 1_800


def orchestration_trace_sample_bucket(task_id: uuid.UUID) -> int:
    """Return a stable task-level bucket in ``[0, 99]`` across processes/restarts."""

    digest = hashlib.sha256(_TRACE_SAMPLE_NAMESPACE + task_id.bytes).digest()
    return int.from_bytes(digest[:8], "big") % 100


def should_deliver_orchestration_trace(
    task_id: uuid.UUID,
    *,
    enabled: bool,
    sample_percent: int,
    anomalous: bool,
    always_include_anomalies: bool,
) -> bool:
    """Apply fail-stable task sampling while optionally retaining diagnostic anomalies."""

    if not enabled:
        return False
    if anomalous and always_include_anomalies:
        return True
    return orchestration_trace_sample_bucket(task_id) < sample_percent


def interpreter_trace_is_anomalous(interpretation: Interpretation, payload: dict[str, Any]) -> bool:
    raw = payload.get("model_task_draft")
    policy_changed = isinstance(raw, dict) and raw != interpretation.task_draft
    return payload.get("repaired") is True or policy_changed


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
            _json_pre(raw, limit=_INTERPRETER_RAW_JSON_LIMIT),
        ]
    )
    if changed:
        lines.extend(
            [
                "",
                "<b>После deterministic policy</b>",
                _json_pre(effective, limit=_INTERPRETER_POLICY_JSON_LIMIT),
            ]
        )
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
