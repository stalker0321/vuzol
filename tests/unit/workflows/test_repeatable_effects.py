"""Repeatability follows permissions, not a read-only label by itself."""

import uuid

from vuzol.storage.models import Step
from vuzol.storage.types import IdempotencyClass
from vuzol.workflows.retry_policy import repeatable_without_new_mutation


def _step(**overrides: object) -> Step:
    values: dict[str, object] = {
        "run_id": uuid.uuid4(),
        "ordinal": 1,
        "step_type": "review",
        "idempotency_class": IdempotencyClass.READ_ONLY,
        "required_capabilities": [],
        "timeout_seconds": 30,
    }
    values.update(overrides)
    return Step(**values)  # type: ignore[arg-type]


def test_read_only_without_mutating_capabilities_can_repeat() -> None:
    assert repeatable_without_new_mutation(_step())


def test_read_only_shell_is_not_treated_as_repeatable() -> None:
    step = _step(required_capabilities=["project_shell"])
    assert not repeatable_without_new_mutation(step)


def test_idempotent_step_needs_a_real_external_key() -> None:
    unlabeled = _step(
        idempotency_class=IdempotencyClass.IDEMPOTENT,
        required_capabilities=["filesystem_write"],
        external_idempotency_key=None,
    )
    keyed = _step(
        idempotency_class=IdempotencyClass.IDEMPOTENT,
        required_capabilities=["filesystem_write"],
        external_idempotency_key="publish-42",
    )
    assert not repeatable_without_new_mutation(unlabeled)
    assert repeatable_without_new_mutation(keyed)


def test_unknown_effects_class_is_not_repeatable() -> None:
    step = _step(idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE)
    assert not repeatable_without_new_mutation(step)
