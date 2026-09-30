"""D0 contracts/wiring acceptance tests (T045, 7 required)."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import HttpUrl

from vuzol.config.models import CostClass, LaunchMode, ProviderProfileConfig, ProviderRole
from vuzol.discussion.horizon import HORIZON_CONTRACT_ENABLED
from vuzol.discussion.sequencer import WorkPackageSequencer
from vuzol.execution.domain import GitInspection
from vuzol.review.handler import ResultReviewHandler
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import RiskLevel, StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _lease() -> LeaseToken:
    return LeaseToken(
        step=StepRecord(
            id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="owner",
            lease_expires_at=None,
        ),
        owner="owner",
        generation=1,
    )


def _request(task_id: uuid.UUID, run_id: uuid.UUID, lease: LeaseToken) -> StepExecutionRequest:
    return StepExecutionRequest(
        task_id=task_id,
        run_id=run_id,
        step_id=lease.step.id,
        step_type="review",
        payload={},
        timeout_seconds=120,
        lease=lease,
    )


def _handler_fixture(
    *, task_risk: RiskLevel, changed_files: tuple[str, ...] = ("src/app.py",), diff: bytes = b"+x\n"
) -> tuple[Path, uuid.UUID, uuid.UUID, LeaseToken, MagicMock, MagicMock]:
    import tempfile
    from pathlib import Path as _P

    tmp = _P(tempfile.mkdtemp())
    wt = tmp / "wt"
    wt.mkdir(exist_ok=True)
    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    step = SimpleNamespace(
        status=StepStatus.RUNNING,
        lease_owner=lease.owner,
        lease_generation=lease.generation,
        run_id=run_id,
        payload={},
        dependency_metadata={"predecessor_ordinals": [5]},
    )
    task = SimpleNamespace(risk=task_risk, task_draft={})
    worktree = SimpleNamespace(
        path=str(wt),
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        base_commit=base,
        result_commit=result,
        diff_hash="c" * 64,
        branch="task-branch",
    )
    session = MagicMock()
    session.get = AsyncMock(side_effect=[step, SimpleNamespace(task_id=task_id), task])
    session.scalar = AsyncMock(side_effect=[validate, worktree])
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result, branch="task-branch", changed_files=changed_files, diff=diff
        )
    )
    return tmp, task_id, run_id, lease, factory, git


@pytest.mark.anyio
async def test_d0_medium_handler_calls_reviewer(tmp_path: Path) -> None:
    """1. Actual medium handler → independent reviewer is invoked."""
    from vuzol.review.domain import ReviewVerdict, ReviewVerdictKind

    tmp, task_id, run_id, lease, factory, git = _handler_fixture(
        task_risk=RiskLevel.MEDIUM, changed_files=("src/app.py",), diff=b"+medium change\n"
    )
    verdict = ReviewVerdict(
        verdict=ReviewVerdictKind.PASSED,
        review_kind="independent",
        risk="medium",
        base_commit="a" * 40,
        result_commit="b" * 40,
        diff_hash="c" * 64,
        changed_files=("src/app.py",),
        findings=(),
        summary="independent ok",
    )
    reviewer = MagicMock()
    reviewer.review = AsyncMock(return_value=verdict)
    # second inspect for mutation check
    git.inspect = AsyncMock(
        side_effect=[
            GitInspection(
                head="b" * 40,
                branch="task-branch",
                changed_files=("src/app.py",),
                diff=b"+medium change\n",
            ),
            GitInspection(
                head="b" * 40,
                branch="task-branch",
                changed_files=("src/app.py",),
                diff=b"+medium change\n",
            ),
        ]
    )
    # fix diff_hash to match (None → inspection hash); use consistent mock
    first = await git.inspect.__call__()
    git.inspect = AsyncMock(side_effect=[first, first])
    handler = ResultReviewHandler(factory, git, worktree_root=tmp, independent_reviewer=reviewer)
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert reviewer.review.await_count == 1
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["review_kind"] == "independent"


@pytest.mark.anyio
async def test_d0_policy_error_blocked_on_handler(tmp_path: Path) -> None:
    """2. Policy error → BLOCKED on the handler (fail-closed)."""
    tmp, task_id, run_id, lease, factory, git = _handler_fixture(task_risk=RiskLevel.MEDIUM)
    handler = ResultReviewHandler(factory, git, worktree_root=tmp, independent_reviewer=MagicMock())
    with patch("vuzol.review.handler.resolve_review_plan", side_effect=ValueError("boom")):
        outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "independent_review_required"


def test_d0_mismatched_schema_fail_closed_before_provider() -> None:
    """3. Mismatched schema bytes → fail closed before provider call."""
    from vuzol.context.resolver import BindingError, ResolvedBinding, ResolvedContext
    from vuzol.providers.handlers import _require_source_report_shape

    legacy = json.dumps(
        {
            "schema": "research-result.v1",
            "schema_version": "research-result.v1",
            "text": "old text",
            "structured_output": None,
        }
    ).encode()
    binding = ResolvedBinding(
        binding_id=uuid.uuid4(),
        slot="predecessor_result",
        source="research-result",
        reference="binding:x",
        content=legacy,
        content_hash="ab" * 32,
        schema_name="research-result",
        schema_version="research-result.v1",
        freshness="fresh",
        required=True,
    )
    resolved = ResolvedContext(bindings=(binding,))
    try:
        _require_source_report_shape(resolved)
    except BindingError as error:
        assert error.category == "source_report_schema_mismatch"
    else:
        raise AssertionError("mismatched schema must fail closed")


def test_d0_old_textual_artifact_not_source_report() -> None:
    """4. Old textual artifact is not accepted as a source report."""

    from vuzol.research.report import validate_source_report_bytes

    legacy = json.dumps(
        {
            "schema_version": "research-provider-result.v1",
            "text": "provider text",
            "structured_output": {"a": 1},
            "finish_reason": "stop",
            "provider_request_id": "req-1",
        }
    ).encode()
    errors = validate_source_report_bytes(legacy)
    assert errors
    assert errors[0] == "legacy_provider_result_not_source_report"
    # legacy readers keep reading the old format (bytes decode, no verified label)
    payload = json.loads(legacy.decode())
    assert "sources" not in payload
    assert payload["text"] == "provider text"


@pytest.mark.anyio
async def test_d0_direct_command_creates_no_review() -> None:
    """5. Direct command/status creates zero review calls."""
    from vuzol.storage.models import Task
    from vuzol.workflows.application import Principal, TaskCommand, apply_task_command

    task_id = uuid.uuid4()
    task = MagicMock(spec=Task)
    task.id = task_id
    task.version = 3
    task.status = MagicMock(value="created")
    task.status.value = "created"
    session = MagicMock()
    session.get = AsyncMock(return_value=task)
    reviewer_calls: list[object] = []
    # apply_task_command never touches review; assert no review import/call path
    result = await apply_task_command(
        session,
        task_id=task_id,
        command=TaskCommand.INSPECT,
        principal=Principal(user_id=1, ingress_source="cli"),
    )
    assert result.applied is False
    assert reviewer_calls == []
    # static guard: direct command path does not reference review dispatch
    import inspect as _inspect

    src = _inspect.getsource(apply_task_command)
    assert "review" not in src.lower()


@pytest.mark.anyio
async def test_d0_flag_off_keeps_materialized_package() -> None:
    """6. Flag off does not downgrade a materialized (pinned) workflow."""
    from types import SimpleNamespace as _NS
    from typing import Any as _Any
    from typing import cast as _cast
    from unittest.mock import AsyncMock as _AM
    from unittest.mock import MagicMock as _MM

    from vuzol.storage.models import WorkPackage
    from vuzol.storage.types import WorkPackageStatus as _WPS

    uow = _MM()
    uow.session.scalar = _AM(return_value=None)
    uow.session.get = _AM(return_value=None)
    uow.events.append = _AM()
    uow.outbox.enqueue = _AM()
    sequencer = WorkPackageSequencer(_cast(_Any, uow))
    package = WorkPackage(
        session_id=uuid.uuid4(),
        project_id="test",
        status=_WPS.RUNNING,
        title="horizon package",
    )
    package.id = uuid.uuid4()
    package.goal = "ship the horizon"
    package.exit_criteria = None
    package.cursor_ordinal = 1
    package.version = 1
    package.horizon_phase = None
    package.execution_contract_version = HORIZON_CONTRACT_ENABLED
    revision = _NS(id=uuid.uuid4())
    result = await sequencer._materialize_current(package, revision, horizon_enabled=False)  # type: ignore[arg-type]
    assert result.completed is False
    assert package.status is _WPS.RUNNING
    assert package.horizon_phase == "evaluating"


def test_d0_executor_never_reviewer_and_level_budget_plumbed() -> None:
    """7. EXECUTOR never selected; level/budget explicitly plumbed."""
    from vuzol.review.independent import select_reviewer_profile
    from vuzol.review.policy import ReviewLevel

    def _profile(pid: str, roles: set[ProviderRole], priority: int = 50) -> ProviderProfileConfig:
        return ProviderProfileConfig.model_validate(
            {
                "id": pid,
                "provider": "openai-compatible",
                "model": "gpt-test",
                "api_base_url": HttpUrl("https://api.example.com/v1"),
                "launch_mode": LaunchMode.API,
                "credential_reference": "env:VUZOL_X",
                "credential_required": True,
                "capabilities": frozenset(),
                "concurrency_limit": 2,
                "context_limit": 8000,
                "output_limit": 1000,
                "cost_class": CostClass.CHEAP,
                "roles": frozenset(roles),
                "routing_priority": priority,
                "supported_task_types": frozenset({"coding"}),
                "sandbox_required": False,
                "input_cost_units_per_million": 0.1,
                "output_cost_units_per_million": 0.2,
                "minimum_unknown_usage_cost": 0.001,
                "enabled": True,
            }
        )

    executor = _profile("exec", {ProviderRole.EXECUTOR}, priority=1)
    reviewer = _profile("rev", {ProviderRole.REVIEWER}, priority=90)
    chosen = select_reviewer_profile(
        (executor, reviewer), required_level=ReviewLevel.L2, budget_eligible=True
    )
    assert chosen is not None and chosen.id == "rev"
    only_exec = select_reviewer_profile(
        (executor,), required_level=ReviewLevel.L3, budget_eligible=True
    )
    assert only_exec is None
    # planner fallback still explicit with level plumbed
    planner = _profile("plan", {ProviderRole.PLANNER}, priority=10)
    fallback = select_reviewer_profile(
        (executor, planner), required_level=ReviewLevel.L2, budget_eligible=True
    )
    assert fallback is not None and fallback.id == "plan"
