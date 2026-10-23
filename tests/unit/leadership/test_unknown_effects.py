"""Unknown external effects remain blocking even on a cancelled task."""

import pytest

from vuzol.leadership.evidence import unknown_effects_block_work
from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import IdempotencyClass, TaskStatus


@pytest.mark.parametrize(
    "step_type",
    [
        "prepare_context",
        "plan",
        "prepare_worktree",
        "execute_code",
        "execute_agent",
        "validate",
        "review",
        "build_static",
    ],
)
@pytest.mark.parametrize("status", [TaskStatus.CANCELLED, TaskStatus.PAUSED, TaskStatus.COMPLETED])
def test_only_cancelled_confined_work_is_nonblocking(step_type, status):
    task = Task(status=status)
    run = Run(workflow_type="coding")
    step = Step(
        step_type=step_type,
        unknown_effects=True,
        idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
        required_capabilities=[],
    )
    assert unknown_effects_block_work(task, run, step) is (status is not TaskStatus.CANCELLED)
    assert step.unknown_effects


@pytest.mark.parametrize(
    "step_type",
    [
        "approval",
        "apply_result",
        "publish_preview",
        "publish_static",
        "ensure_capabilities",
        "ensure_dependencies",
        "deploy",
        "external_write",
        "future_step",
    ],
)
@pytest.mark.parametrize("idempotency", list(IdempotencyClass))
def test_external_and_unknown_step_types_fail_closed(step_type, idempotency):
    assert unknown_effects_block_work(
        Task(status=TaskStatus.CANCELLED),
        Run(workflow_type="coding"),
        Step(
            step_type=step_type,
            unknown_effects=True,
            idempotency_class=idempotency,
            required_capabilities=[],
        ),
    )


def test_external_capabilities_and_other_workflows_are_not_confined():
    step = Step(
        step_type="execute_code",
        unknown_effects=True,
        idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
        required_capabilities=["network"],
    )
    task = Task(status=TaskStatus.CANCELLED)
    assert unknown_effects_block_work(task, Run(workflow_type="coding"), step)
    step.required_capabilities = []
    assert unknown_effects_block_work(task, Run(workflow_type="infrastructure"), step)
