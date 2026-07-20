"""Focused unit coverage for planner validation and executor context handoff."""

from __future__ import annotations

import hashlib
import uuid
from decimal import Decimal

import pytest

from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus
from vuzol.providers.planner_handoff import (
    MAX_PLANNER_CONTEXT_CHARS,
    PLANNER_CONTEXT_SOURCE,
    PLANNER_HANDOFF_FENCED_CATEGORY,
    PlannerHandoffFenced,
    PlannerResultUnusable,
    assess_persisted_plan_result,
    assess_planner_provider_result,
    bound_planner_text,
    build_planner_context_items,
    describe_planner_handoff,
    load_planner_context_for_run,
    redact_planner_text,
)
from vuzol.providers.routing import PROVIDER_STEP_ROLES
from vuzol.storage.models import Step
from vuzol.storage.types import IdempotencyClass, QueueClass, RetryClass, StepStatus
from vuzol.workflows.definitions import WORKFLOW_REGISTRY


def _usage() -> NormalizedUsage:
    return NormalizedUsage(duration_ms=10)


def _result(**changes: object) -> ProviderResult:
    values: dict[str, object] = {
        "status": ProviderResultStatus.SUCCEEDED,
        "text": "1. Inspect files\n2. Edit carefully\n3. Validate",
        "usage": _usage(),
        "finish_reason": "stop",
        "adapter_version": "test.v1",
    }
    values.update(changes)
    return ProviderResult.model_validate(values)


def _plan_step(
    *,
    status: StepStatus = StepStatus.COMPLETED,
    result: dict[str, object] | None = None,
) -> Step:
    return Step(
        id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        ordinal=1,
        dependency_metadata={},
        step_type="plan",
        queue_class=QueueClass.LIGHT,
        status=status,
        required_capabilities=[],
        payload={},
        result=result
        if result is not None
        else {
            "text": "Inspect then edit",
            "finish_reason": "stop",
            "handoff": {"status": "ready"},
        },
        retry_class=RetryClass.TRANSIENT,
        idempotency_class=IdempotencyClass.READ_ONLY,
        max_attempts=3,
        timeout_seconds=600,
    )


def test_valid_plan_text_is_accepted() -> None:
    assert assess_planner_provider_result(_result()).startswith("1. Inspect")


def test_empty_output_is_not_a_successful_plan() -> None:
    with pytest.raises(PlannerResultUnusable) as error:
        assess_planner_provider_result(_result(text="   ", finish_reason="stop"))
    assert error.value.category == "planner_empty_output"


def test_token_truncation_is_not_a_successful_plan() -> None:
    with pytest.raises(PlannerResultUnusable) as error:
        assess_planner_provider_result(
            _result(text="partial plan that hit the limit", finish_reason="length")
        )
    assert error.value.category == "planner_truncated"


def test_structured_only_output_is_rejected() -> None:
    with pytest.raises(PlannerResultUnusable) as error:
        assess_planner_provider_result(
            _result(text=None, structured_output={"steps": ["a"]}, finish_reason="stop")
        )
    assert error.value.category == "planner_invalid_output"


def test_no_plan_step_yields_empty_context() -> None:
    assert load_planner_context_for_run(None) == ()


def test_successful_handoff_builds_bounded_redacted_context() -> None:
    # Synthetic secret-shaped span for redaction coverage (not a real credential).
    secret_span = "Authorization: Bearer sk-" + ("a" * 24)  # pragma: allowlist secret
    plan = f"Do the work carefully. {secret_span}\nThen validate."
    step = _plan_step(
        result={"text": plan, "finish_reason": "stop", "handoff": {"status": "ready"}}
    )
    items = load_planner_context_for_run(step, redaction_patterns=(r"sk-[A-Za-z0-9]+",))
    assert len(items) == 1
    assert items[0].source == PLANNER_CONTEXT_SOURCE
    assert "Do the work carefully" in items[0].content
    assert "Bearer" not in items[0].content or "[REDACTED]" in items[0].content
    assert "sk-" + ("a" * 24) not in items[0].content
    assert items[0].content_hash == hashlib.sha256(items[0].content.encode()).hexdigest()


def test_stale_incomplete_plan_is_fenced() -> None:
    step = _plan_step(status=StepStatus.FAILED, result={"text": "old", "finish_reason": "stop"})
    with pytest.raises(PlannerHandoffFenced, match="fenced") as error:
        load_planner_context_for_run(step)
    assert error.value.category == PLANNER_HANDOFF_FENCED_CATEGORY


def test_stale_completed_empty_plan_is_fenced() -> None:
    step = _plan_step(
        status=StepStatus.COMPLETED,
        result={"text": "", "finish_reason": "stop", "handoff": {"status": "ready"}},
    )
    with pytest.raises(PlannerHandoffFenced, match="fenced") as error:
        load_planner_context_for_run(step)
    assert error.value.reason == "planner_empty_output"


def test_stale_completed_truncated_plan_is_fenced() -> None:
    step = _plan_step(
        status=StepStatus.COMPLETED,
        result={"text": "half", "finish_reason": "length"},
    )
    with pytest.raises(PlannerHandoffFenced, match="fenced") as error:
        load_planner_context_for_run(step)
    assert error.value.reason == "planner_truncated"


def test_non_succeeded_provider_status_is_rejected() -> None:
    with pytest.raises(PlannerResultUnusable) as error:
        assess_planner_provider_result(
            _result(status=ProviderResultStatus.FAILED, text="looks fine")
        )
    assert error.value.category == "planner_invalid_output"


def test_infrastructure_plan_is_not_a_provider_executor_consumer() -> None:
    """Product decision: infrastructure plan is approval/human context only."""
    definition = WORKFLOW_REGISTRY["infrastructure.v1"]
    plan_keys = {
        step.key for step in definition.steps if step.step_type == "plan" or step.key == "plan"
    }
    assert plan_keys
    provider_consumers = frozenset({"execute_code", "execute_agent"})
    consumer_steps = [step for step in definition.steps if step.step_type in provider_consumers]
    assert consumer_steps == []
    for step in definition.steps:
        if step.step_type == "privileged_execute":
            assert step.step_type not in PROVIDER_STEP_ROLES


def test_context_items_are_bounded() -> None:
    huge = "x" * (MAX_PLANNER_CONTEXT_CHARS + 5_000)
    items = build_planner_context_items(huge, plan_step_id=str(uuid.uuid4()))
    total = sum(len(item.content) for item in items)
    assert total <= MAX_PLANNER_CONTEXT_CHARS
    assert bound_planner_text(huge).endswith("…")


def test_redact_planner_text_masks_common_secrets() -> None:
    assert "[REDACTED]" in redact_planner_text("token=super-secret-value")
    assert "super-secret-value" not in redact_planner_text("token=super-secret-value")


def test_describe_handoff_for_ready_failed_and_retry() -> None:
    ready = _plan_step()
    assert "подключён" in describe_planner_handoff(ready)

    failed = _plan_step(
        status=StepStatus.FAILED,
        result={
            "text": "",
            "finish_reason": "length",
            "handoff": {"status": "rejected", "reason": "planner_truncated"},
        },
    )
    failed.failure_category = "planner_truncated"
    assert "не выполнен" in describe_planner_handoff(failed)
    assert "planner_truncated" in describe_planner_handoff(failed)

    retrying = _plan_step(
        status=StepStatus.QUEUED,
        result={
            "text": "",
            "finish_reason": "stop",
            "handoff": {"status": "rejected", "reason": "planner_empty_output"},
        },
    )
    assert "retry" in describe_planner_handoff(retrying)


def test_assess_persisted_plan_rejects_missing_payload() -> None:
    with pytest.raises(PlannerResultUnusable) as error:
        assess_persisted_plan_result(None)
    assert error.value.category == "planner_missing_result"


def test_provider_result_cost_fields_unused_but_valid() -> None:
    # Keep NormalizedUsage exercised with optional accounting fields.
    usage = NormalizedUsage(
        input_tokens=1,
        output_tokens=2,
        cost_units=Decimal("0.01"),
        duration_ms=5,
    )
    result = _result(usage=usage)
    assert assess_planner_provider_result(result)
