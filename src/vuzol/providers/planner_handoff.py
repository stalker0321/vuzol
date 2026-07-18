"""Validate planner outcomes and hand validated plan text into executor context.

The handoff uses the persisted plan step result already stored on the workflow run.
It does not invent a second plan store or out-of-band channel.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from vuzol.providers.domain import ContextItem, ProviderResult, ProviderResultStatus
from vuzol.storage.models import Step
from vuzol.storage.types import StepStatus

PLANNER_CONTEXT_SOURCE = "workflow_plan_result"
PLANNER_HANDOFF_FENCED_CATEGORY = "planner_handoff_fenced"
# Stay under ContextItem.content max (20_000) and keep total handoff bounded.
MAX_PLANNER_CONTEXT_CHARS = 12_000
MAX_CONTEXT_ITEM_CHARS = 8_000
_DEFAULT_SECRET_PATTERNS = (
    r"(?i)(api[_-]?key|authorization|bearer|token|password|secret)\s*[:=]\s*\S+",
    r"(?i)sk-[A-Za-z0-9]{16,}",
)


class PlannerResultUnusable(ValueError):
    """Planner provider output cannot be treated as a successful plan."""

    def __init__(self, category: str, summary: str) -> None:
        super().__init__(summary)
        self.category = category
        self.summary = summary


class PlannerHandoffFenced(LookupError):
    """Executor build refused a non-usable or incomplete plan step."""

    def __init__(self, summary: str, *, reason: str = PLANNER_HANDOFF_FENCED_CATEGORY) -> None:
        super().__init__(summary)
        self.category = PLANNER_HANDOFF_FENCED_CATEGORY
        self.reason = reason
        self.summary = summary


def assess_planner_provider_result(result: ProviderResult) -> str:
    """Return non-empty plan text or raise PlannerResultUnusable."""

    if result.status is not ProviderResultStatus.SUCCEEDED:
        raise PlannerResultUnusable(
            "planner_invalid_output",
            f"planner provider status is {result.status.value}, not succeeded",
        )
    if result.finish_reason == "length":
        raise PlannerResultUnusable(
            "planner_truncated",
            "planner output stopped at the token limit",
        )
    text = result.text.strip() if isinstance(result.text, str) else ""
    if not text:
        structured = result.structured_output
        if isinstance(structured, dict) and structured:
            # Free-form planning is text-first; structured-only bodies are not a usable plan.
            raise PlannerResultUnusable(
                "planner_invalid_output",
                "planner returned structured output without plan text",
            )
        raise PlannerResultUnusable(
            "planner_empty_output",
            "planner returned empty output",
        )
    return text


def assess_persisted_plan_result(result: dict[str, Any] | None) -> str:
    """Re-check a stored plan step result before attaching it to an executor request."""

    if not isinstance(result, dict):
        raise PlannerResultUnusable(
            "planner_missing_result",
            "completed plan step has no result payload",
        )
    if result.get("finish_reason") == "length":
        raise PlannerResultUnusable(
            "planner_truncated",
            "stored plan result is token-truncated",
        )
    text = result.get("text")
    plan = text.strip() if isinstance(text, str) else ""
    if not plan:
        raise PlannerResultUnusable(
            "planner_empty_output",
            "stored plan result has empty text",
        )
    return plan


def redact_planner_text(text: str, patterns: tuple[str, ...] = ()) -> str:
    """Replace secret-shaped spans before plan text reaches an executor."""

    redacted = text
    for pattern in (*_DEFAULT_SECRET_PATTERNS, *patterns):
        try:
            compiled = re.compile(pattern)
        except re.error:
            continue
        redacted = compiled.sub("[REDACTED]", redacted)
    return redacted


def bound_planner_text(text: str, *, max_chars: int = MAX_PLANNER_CONTEXT_CHARS) -> str:
    normalized = text.strip()
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 1].rstrip() + "…"


def build_planner_context_items(
    plan_text: str,
    *,
    plan_step_id: str,
    redaction_patterns: tuple[str, ...] = (),
) -> tuple[ContextItem, ...]:
    """Build bounded, redacted ContextItems from validated plan text."""

    safe = bound_planner_text(redact_planner_text(plan_text, redaction_patterns))
    if not safe:
        raise PlannerResultUnusable(
            "planner_empty_output",
            "plan text is empty after redaction and bounding",
        )
    chunks = tuple(
        safe[offset : offset + MAX_CONTEXT_ITEM_CHARS]
        for offset in range(0, len(safe), MAX_CONTEXT_ITEM_CHARS)
    )
    total = len(chunks)
    return tuple(
        ContextItem(
            source=PLANNER_CONTEXT_SOURCE,
            reference=f"step:{plan_step_id}:plan:part-{index}-of-{total}",
            content=chunk,
            content_hash=hashlib.sha256(chunk.encode()).hexdigest(),
        )
        for index, chunk in enumerate(chunks, start=1)
    )


def load_planner_context_for_run(
    plan_step: Step | None,
    *,
    redaction_patterns: tuple[str, ...] = (),
) -> tuple[ContextItem, ...]:
    """Resolve planner context from the run's plan step, or empty when planning was skipped.

    Stale fencing: a present plan step must be COMPLETED with a still-usable result.
    Incomplete, failed, or corrupted plan results fail closed instead of silently
    executing without the planned context.
    """

    if plan_step is None:
        return ()
    if plan_step.step_type != "plan":
        raise PlannerHandoffFenced(
            "planner handoff requires a plan step",
            reason="planner_missing_result",
        )
    if plan_step.status is not StepStatus.COMPLETED:
        raise PlannerHandoffFenced(
            f"planner handoff fenced: plan step status is {plan_step.status.value}",
            reason=f"plan_status_{plan_step.status.value}",
        )
    result = plan_step.result if isinstance(plan_step.result, dict) else None
    try:
        plan_text = assess_persisted_plan_result(result)
        return build_planner_context_items(
            plan_text,
            plan_step_id=str(plan_step.id),
            redaction_patterns=redaction_patterns,
        )
    except PlannerResultUnusable as error:
        raise PlannerHandoffFenced(
            f"planner handoff fenced: {error.summary}",
            reason=error.category,
        ) from error


def planner_result_payload(
    *,
    profile_id: str,
    model: str,
    provider_request_id: str | None,
    text: str | None,
    structured_output: dict[str, Any] | None,
    finish_reason: str | None,
    handoff_status: str,
    handoff_reason: str | None = None,
) -> dict[str, Any]:
    """Persist the same planner result shape used by traces and executor handoff."""

    payload: dict[str, Any] = {
        "profile_id": profile_id,
        "model": model,
        "provider_request_id": provider_request_id,
        "text": text,
        "structured_output": structured_output,
        "finish_reason": finish_reason,
        "handoff": {"status": handoff_status},
    }
    if handoff_reason is not None:
        payload["handoff"]["reason"] = handoff_reason
    return payload


def describe_planner_handoff(step: Step) -> str:
    """Human-readable handoff state for the orchestration planner trace."""

    result: dict[str, Any] = step.result if isinstance(step.result, dict) else {}
    raw_handoff = result.get("handoff")
    handoff: dict[str, Any] = raw_handoff if isinstance(raw_handoff, dict) else {}
    status = handoff.get("status")
    if step.status is StepStatus.COMPLETED and status == "ready":
        return "Handoff воркеру: <b>подключён</b> (bounded plan context входит в ProviderRequest)"
    if step.status is StepStatus.COMPLETED:
        # Completed before handoff metadata existed, or completed without ready mark.
        try:
            assess_persisted_plan_result(result if isinstance(result, dict) else None)
            return (
                "Handoff воркеру: <b>подключён</b> (bounded plan context входит в ProviderRequest)"
            )
        except PlannerResultUnusable:
            return "Handoff воркеру: <b>заблокирован</b> (completed plan result is not usable)"
    if step.status is StepStatus.QUEUED:
        return "Handoff воркеру: <b>ожидает retry</b> (plan ещё не usable)"
    if step.status in {StepStatus.FAILED, StepStatus.BLOCKED, StepStatus.CANCELLED}:
        reason = handoff.get("reason") or step.failure_category or "plan unusable"
        return f"Handoff воркеру: <b>не выполнен</b> (<code>{reason}</code>)"
    return "Handoff воркеру: <b>не готов</b>"
