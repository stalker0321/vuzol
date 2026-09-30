"""D2 acceptance/promotion unit tests (no PostgreSQL)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import StepStatus
from vuzol.workflows.acceptance import (
    AcceptanceGateHandler,
    evidence_hash,
    validate_evidence,
)
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


def _doc(**overrides: object) -> dict[str, Any]:
    base: dict[str, Any] = {
        "schema": "acceptance-evidence.v1",
        "package_id": str(uuid.uuid4()),
        "plan_revision_id": str(uuid.uuid4()),
        "plan_content_hash": "ab" * 32,
        "goal": "ship it",
        "goal_revision": 1,
        "spec_revision": None,
        "configuration_revision": "c" * 64,
        "policy_revision": "d" * 64,
        "integration_base_head": "e" * 40,
        "result_commit": "f" * 40,
        "criteria": [{"criterion_id": "a", "satisfied": True}],
        "test_results": [{"name": "gate", "exit_code": 0}],
        "review_refs": ["aa" * 32],
        "unresolved_caveats": [],
        "unresolved_effects": [],
        "created_at": "2026-09-30T00:00:00+00:00",
    }
    base.update(overrides)
    return base


def test_d2_evidence_schema_file_matches_validator() -> None:
    """L2: the frozen schema file and the code validator agree."""

    import json
    from pathlib import Path

    schema = json.loads(
        Path("docs/schemas/acceptance-evidence.v1.schema.json").read_text()
    )
    assert schema["title"] == "Acceptance evidence v1"
    assert set(schema["required"]) >= {
        "package_id",
        "criteria",
        "review_refs",
        "integration_base_head",
        "result_commit",
    }
    assert validate_evidence(_doc()) == ()
    assert validate_evidence([]) == ("evidence_not_object",)
    assert validate_evidence({}) == ("evidence_schema_mismatch",)
    assert validate_evidence(_doc(criteria=[])) == ("evidence_criteria_missing",)
    assert validate_evidence(_doc(review_refs=[])) == ("evidence_review_refs_missing",)
    frozen = _doc()
    assert evidence_hash(frozen) == evidence_hash(dict(frozen))
    assert evidence_hash(frozen) != evidence_hash(_doc())


def test_d2_horizon_runtime_doc_single_statement() -> None:
    """pp.12: the doc carries one C8-consistent statement (dossier Q-C8)."""

    from pathlib import Path

    text = Path("docs/HORIZON_RUNTIME.md").read_text()
    assert "Flag off = legacy COMPLETED; flag on = acceptance required" in text


def test_d2_horizon_status_mapping_acceptance() -> None:
    """L5: the frozen mapping separates evaluating/accepted/succeeded."""

    from vuzol.discussion.horizon import HORIZON_STATUS_MAPPING, horizon_status
    from vuzol.storage.types import WorkPackageStatus

    assert (
        horizon_status(WorkPackageStatus.RUNNING, "evaluating", False)
        == HORIZON_STATUS_MAPPING["evaluating"]
    )
    assert (
        horizon_status(WorkPackageStatus.COMPLETED, None, True)
        == HORIZON_STATUS_MAPPING["succeeded"]
    )
    assert (
        horizon_status(WorkPackageStatus.COMPLETED, None, False)
        == HORIZON_STATUS_MAPPING["running"]
    )


@pytest.mark.anyio
async def test_d2_record_acceptance_rejects_bare_accept() -> None:
    """pp.2 (E06): accepted=True with no evidence is impossible."""

    from vuzol.discussion.domain import DomainError
    from vuzol.discussion.service import WorkPackageService
    from vuzol.storage.models import WorkPackage
    from vuzol.storage.types import WorkPackageStatus

    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="vuzol",
        status=WorkPackageStatus.RUNNING,
        title="horizon",
    )
    package.id = uuid.uuid4()
    package.goal = "ship it"
    package.exit_criteria = None
    package.version = 4
    package.cursor_ordinal = None
    package.running_revision_id = uuid.uuid4()
    package.head_revision_id = package.running_revision_id
    package.horizon_phase = "evaluating"
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_fenced_revision = AsyncMock(
        return_value=SimpleNamespace(id=package.running_revision_id)
    )
    uow.session = MagicMock()
    service = WorkPackageService(cast(Any, uow))
    with pytest.raises(DomainError, match="acceptance_evidence_missing"):
        await service.record_acceptance(
            package_id=package.id,
            revision_number=1,
            h8="ab" * 8,
            expected_status_generation=4,
            accepted=True,
            artifact_id=None,
            user_id=7,
            horizon_enabled=True,
        )


@pytest.mark.anyio
async def test_d2_decide_rejects_config_drift() -> None:
    """pp.9: envelope revisions drifted since request → decide fails closed."""

    from datetime import UTC, datetime, timedelta

    from vuzol.storage.types import ApprovalStatus, StepStatus
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
    session.scalar = AsyncMock(side_effect=[approval, step, run, task])
    with pytest.raises(ValueError, match="drifted since request"):
        await decide_result(
            session, approval_id, decision="approve", deciding_user_id=7
        )


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


def _accept_request() -> tuple[StepExecutionRequest, LeaseToken, CancellationContext]:
    import uuid as _uuid

    lease = LeaseToken(
        step=StepRecord(
            id=_uuid.uuid4(),
            run_id=_uuid.uuid4(),
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="owner",
            lease_expires_at=None,
        ),
        owner="owner",
        generation=1,
    )
    return (
        StepExecutionRequest(
            task_id=_uuid.uuid4(),
            run_id=lease.step.run_id,
            step_id=lease.step.id,
            step_type="acceptance",
            payload={},
            timeout_seconds=60,
            lease=lease,
        ),
        lease,
        CancellationContext(),
    )


@pytest.mark.anyio
async def test_d2_acceptance_passes_through_without_link() -> None:
    """L1: plain coding tasks (no package link) pass the acceptance step."""

    request, _lease, cancellation = _accept_request()
    step = SimpleNamespace(
        status="running",
        lease_owner="owner",
        lease_generation=1,
        run_id=request.run_id,
    )
    run = SimpleNamespace(task_id=request.task_id)
    task = SimpleNamespace(id=request.task_id)
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[step, run, task],
    )
    session.scalar = AsyncMock(return_value=None)
    handler = AcceptanceGateHandler(MagicMock(return_value=_Ctx(session)))
    outcome = await handler.execute(request, cancellation)
    assert outcome.kind.value == "succeeded"
    assert outcome.result["acceptance_evidence_id"] is None


@pytest.mark.anyio
async def test_d2_acceptance_lease_mismatch_blocked() -> None:
    """L1: unfenced acceptance execution fails closed."""

    from vuzol.workflows.acceptance import AcceptanceGateHandler
    from vuzol.workflows.domain import OutcomeKind

    request, _lease, cancellation = _accept_request()
    step = SimpleNamespace(
        status="running",
        lease_owner="someone-else",
        lease_generation=1,
        run_id=request.run_id,
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[
            step,
            SimpleNamespace(),
            SimpleNamespace(),
        ]
    )
    handler = AcceptanceGateHandler(MagicMock(return_value=_Ctx(session)))
    outcome = await handler.execute(request, cancellation)
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "acceptance_evidence_missing"
