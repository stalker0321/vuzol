import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.storage.types import RunStatus, StepStatus
from vuzol.workflows.service import (
    _resume_review_after_validation,
    activate_ready_steps,
    finalize_if_complete,
    start_run,
)


@pytest.mark.anyio
async def test_start_run_rejects_terminal_run() -> None:
    run = SimpleNamespace(status=RunStatus.COMPLETED)

    with pytest.raises(ValueError, match="cannot start"):
        await start_run(cast(Any, MagicMock()), cast(Any, run), actor_type="test")


@pytest.mark.anyio
async def test_activate_ready_steps_ignores_non_running_run() -> None:
    run = SimpleNamespace(status=RunStatus.CREATED)
    assert await activate_ready_steps(cast(Any, MagicMock()), cast(Any, run)) == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "payload",
    [{}, {"resume_after_validation_step_id": 7}, {"resume_after_validation_step_id": "bad"}],
)
async def test_resume_review_rejects_missing_or_invalid_target(payload: dict[str, object]) -> None:
    session = MagicMock()
    session.scalar = AsyncMock()
    validation = SimpleNamespace(payload=payload)

    assert not await _resume_review_after_validation(
        cast(Any, session), cast(Any, SimpleNamespace(id=uuid.uuid4())), cast(Any, validation)
    )
    cast(AsyncMock, session.scalar).assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "target",
    [
        None,
        SimpleNamespace(step_type="validate", status=StepStatus.BLOCKED),
        SimpleNamespace(step_type="review", status=StepStatus.COMPLETED),
    ],
)
async def test_resume_review_fails_closed_for_wrong_target(target: object) -> None:
    session = MagicMock()
    session.scalar = AsyncMock(return_value=target)
    validation = SimpleNamespace(payload={"resume_after_validation_step_id": str(uuid.uuid4())})

    assert not await _resume_review_after_validation(
        cast(Any, session), cast(Any, SimpleNamespace(id=uuid.uuid4())), cast(Any, validation)
    )


@pytest.mark.anyio
async def test_resume_review_queues_target_and_extends_exhausted_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = SimpleNamespace(
        step_type="review",
        status=StepStatus.BLOCKED,
        attempt_count=2,
        max_attempts=2,
        failure_category="review_failed",
        failure_summary="failed",
        payload={"old": True},
    )
    session = MagicMock()
    session.scalar = AsyncMock(return_value=target)
    transition = AsyncMock()
    monkeypatch.setattr("vuzol.workflows.service.transition_step", transition)
    validation_id = uuid.uuid4()
    validation = SimpleNamespace(
        id=validation_id,
        payload={"resume_after_validation_step_id": str(uuid.uuid4())},
    )

    assert await _resume_review_after_validation(
        cast(Any, session), cast(Any, SimpleNamespace(id=uuid.uuid4())), cast(Any, validation)
    )
    transition.assert_awaited_once()
    assert target.max_attempts == 3
    assert target.failure_category is None
    assert target.failure_summary is None
    assert target.payload["repair_validation_step_id"] == str(validation_id)


@pytest.mark.anyio
async def test_finalize_requires_steps_and_running_state(monkeypatch: pytest.MonkeyPatch) -> None:
    session = MagicMock()
    run = SimpleNamespace(id=uuid.uuid4(), status=RunStatus.RUNNING)
    monkeypatch.setattr("vuzol.workflows.service._steps_for_run", AsyncMock(return_value=()))
    assert not await finalize_if_complete(cast(Any, session), cast(Any, run))

    monkeypatch.setattr(
        "vuzol.workflows.service._steps_for_run",
        AsyncMock(return_value=(SimpleNamespace(status=StepStatus.QUEUED),)),
    )
    assert not await finalize_if_complete(cast(Any, session), cast(Any, run))

    run.status = RunStatus.BLOCKED
    monkeypatch.setattr(
        "vuzol.workflows.service._steps_for_run",
        AsyncMock(return_value=(SimpleNamespace(status=StepStatus.COMPLETED),)),
    )
    assert not await finalize_if_complete(cast(Any, session), cast(Any, run))


@pytest.mark.anyio
async def test_finalize_completes_running_run(monkeypatch: pytest.MonkeyPatch) -> None:
    run = SimpleNamespace(id=uuid.uuid4(), status=RunStatus.RUNNING, ended_at=None)
    monkeypatch.setattr(
        "vuzol.workflows.service._steps_for_run",
        AsyncMock(return_value=(SimpleNamespace(status=StepStatus.COMPLETED),)),
    )
    transition = AsyncMock()
    monkeypatch.setattr("vuzol.workflows.service.transition_run", transition)

    assert await finalize_if_complete(cast(Any, MagicMock()), cast(Any, run))
    transition.assert_awaited_once()
    assert run.ended_at is not None
