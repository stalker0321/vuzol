"""D2 acceptance/promotion unit tests (no PostgreSQL)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.storage.types import StepStatus


@pytest.mark.anyio
async def test_d2_decide_rejects_config_drift() -> None:
    """pp.9: envelope revisions drifted since request → decide fails closed."""

    from datetime import UTC, datetime, timedelta

    from vuzol.storage.types import ApprovalStatus
    from vuzol.workflows.controls import decide_result
    from vuzol.workflows.result_approval import envelope_hash

    approval_id = uuid.uuid4()
    step_id = uuid.uuid4()
    run_id = uuid.uuid4()
    task_id = uuid.uuid4()
    envelope = {
        "schema_version": "result-approval.v1",
        "step_id": str(step_id),
        "configuration_revision": "c" * 64,
        "policy_revision": "d" * 64,
    }
    digest = envelope_hash(envelope)
    approval = SimpleNamespace(
        id=approval_id,
        step_id=step_id,
        action_envelope_hash=digest,
        status=ApprovalStatus.PENDING,
        requested_action="apply_result",
        expires_at=datetime.now(UTC) + timedelta(days=1),
        deciding_user_id=None,
        decided_at=None,
    )
    step = SimpleNamespace(
        id=step_id,
        run_id=run_id,
        status=StepStatus.WAITING_APPROVAL,
        step_type="approval",
        payload={"action_envelope": envelope},
    )
    run = SimpleNamespace(
        id=run_id,
        task_id=task_id,
        configuration_revision="c" * 63 + "X",
        policy_revision="d" * 64,
    )
    task = SimpleNamespace(id=task_id)
    session = MagicMock()
    located = MagicMock()
    located.one_or_none.return_value = (task_id, run_id)
    session.execute = AsyncMock(return_value=located)
    session.scalar = AsyncMock(side_effect=[step_id, task, run, step, approval])
    with pytest.raises(ValueError, match="drifted since request"):
        await decide_result(session, approval_id, decision="approve", deciding_user_id=7)


@pytest.mark.anyio
async def test_d2_expire_pending_approvals() -> None:
    """L4: revise/approve path expires stale pending approvals (unit)."""

    from vuzol.discussion.service import WorkPackageService

    pending = SimpleNamespace(status="pending", expires_at=None)
    uow = MagicMock()
    uow.session.scalars = AsyncMock(
        side_effect=[
            SimpleNamespace(all=MagicMock(return_value=[uuid.uuid4()])),  # tasks
            SimpleNamespace(all=MagicMock(return_value=[uuid.uuid4()])),  # runs
            SimpleNamespace(all=MagicMock(return_value=[uuid.uuid4()])),  # steps
            SimpleNamespace(all=MagicMock(return_value=[pending])),  # approvals
        ]
    )
    uow.session.scalar = AsyncMock()
    uow.events.append = AsyncMock()
    service = WorkPackageService(cast(Any, uow))
    count = await service._expire_pending_approvals(uuid.uuid4())
    assert count == 1
    assert pending.expires_at is not None


@pytest.mark.anyio
async def test_d2_block_for_attention_records_corrective() -> None:
    """pp.11: workflow BLOCKED leaves a durable corrective trace (unit)."""

    from vuzol.storage.models import Run, Step
    from vuzol.workflows.service import _block_for_attention

    run = MagicMock(spec=Run)
    run.id = uuid.uuid4()
    run.task_id = uuid.uuid4()
    step = MagicMock(spec=Step)
    step.id = uuid.uuid4()
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    with (
        __import__("unittest.mock", fromlist=["patch"]).patch(
            "vuzol.workflows.service.transition_step", new=AsyncMock()
        ),
        __import__("unittest.mock", fromlist=["patch"]).patch(
            "vuzol.workflows.service.transition_run", new=AsyncMock()
        ),
    ):
        await _block_for_attention(session, run, step)
    kinds = [call.args[0] for call in session.add.call_args_list]
    events = [item for item in kinds if getattr(item, "event_type", "") != ""]
    assert any("correction_required" in getattr(item, "event_type", "") for item in events)
    # task scope carries the Event trace; projection outbox rows are for
    # package scope only (terminal task projections already fire in commit)
    outboxes = [item for item in kinds if getattr(item, "destination", "") != ""]
    assert not outboxes


def test_d2_compose_has_applier() -> None:
    """pp.7/Q5: dev parity — applier service exists in compose (file only)."""

    from pathlib import Path

    text = Path("compose.yaml").read_text()
    assert "vuzol-applier" in text
    assert "applier:" in text


class _Ctx:
    def __init__(self, session: MagicMock) -> None:
        self._session = session

    async def __aenter__(self) -> MagicMock:
        return self._session

    async def __aexit__(self, *_args: object) -> None:
        return None
