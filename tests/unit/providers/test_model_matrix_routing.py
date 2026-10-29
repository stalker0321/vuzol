"""Run-pinned routes retain launch mode, authorization, and exact-profile retries."""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unit.projects.test_model_matrix import assignment, bundle, configured
from vuzol.config.models import Capability, LaunchMode
from vuzol.config.settings import HardLimits
from vuzol.projects.model_matrix import MATRIX_KEY
from vuzol.providers import routing
from vuzol.providers.budgets import GoalBudgetExceeded
from vuzol.providers.domain import EffectiveProfileState
from vuzol.storage.models import Run, Step, Task
from vuzol.storage.types import QueueClass, RunStatus, StepStatus


@pytest.fixture
def route_call(monkeypatch):
    selected = configured("codex")
    alternate = configured("codex", profile_id="alternate")
    task = Task(id=uuid.uuid4(), project_id="project", task_type="coding", budget_epoch=0)
    run = Run(
        id=uuid.uuid4(),
        task_id=task.id,
        status=RunStatus.RUNNING,
        budget_mode="balanced",
        selected_route={MATRIX_KEY: {"executor": assignment(selected).model_dump(mode="json")}},
        configuration_revision="config",
    )
    step = Step(
        id=uuid.uuid4(),
        run_id=run.id,
        step_type="execute_code",
        queue_class=QueueClass.LIGHT,
        status=StepStatus.QUEUED,
        payload={"retry_failed_profile_id": selected.id},
        executor_profile_id=selected.id,
        required_capabilities=[],
        lease_generation=2,
        attempt_count=1,
        failure_category="provider_unavailable",
    )
    registries = bundle(selected, alternate)
    registries.projects.get.return_value.allowed_capabilities = frozenset(Capability)
    settings = MagicMock()
    settings.limits = HardLimits()
    settings.execution.default_executor_profile_id = None
    session = AsyncMock()
    session.execute.side_effect = [None, SimpleNamespace(all=lambda: [(step, run, task)])]
    session.scalar.side_effect = [1, 1]

    async def refresh(row, **_kwargs):
        row.heartbeat_at = datetime.now(UTC)
        row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)

    session.refresh.side_effect = refresh
    reserve = AsyncMock(return_value=SimpleNamespace(id=uuid.uuid4(), reserved_output_tokens=100))
    persist = AsyncMock()
    blocked = AsyncMock()
    monkeypatch.setattr(routing, "execution_constraints_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(routing, "_states", AsyncMock(return_value={}))
    monkeypatch.setattr(routing, "_estimated_input_tokens", lambda *_args: 100)
    monkeypatch.setattr(routing, "remaining_output_allowance", AsyncMock(return_value=100))
    monkeypatch.setattr(routing, "reserve_budget", reserve)
    monkeypatch.setattr(routing, "resolve_horizon_scope", AsyncMock(return_value=None))
    monkeypatch.setattr(routing, "_persist_decision", persist)
    monkeypatch.setattr(routing, "_block_route", blocked)
    monkeypatch.setattr(
        "vuzol.discussion.work_context.load_work_context", AsyncMock(return_value=())
    )
    monkeypatch.setattr(routing, "ceiling_is_admission_only", lambda _profile: False)
    return SimpleNamespace(
        profile=selected,
        alternate=alternate,
        task=task,
        run=run,
        step=step,
        registries=registries,
        settings=settings,
        session=session,
        reserve=reserve,
        persist=persist,
        blocked=blocked,
    )


async def claim(c):
    return await routing.claim_routed_step(
        c.session,
        settings=c.settings,
        registries=c.registries,
        owner="host",
        lease_seconds=30,
        candidate_limit=1,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("provider", ["codex", "grok", "kimi", "pi"])
async def test_pinned_executor_route_uses_selected_launch_mode_without_fallback(
    route_call, provider
):
    c = route_call
    selected = configured(provider).model_copy(update={"fallback_profile_ids": (c.alternate.id,)})
    c.registries = bundle(selected, c.alternate)
    c.registries.projects.get.return_value.allowed_capabilities = frozenset(Capability)
    c.run.selected_route[MATRIX_KEY]["executor"] = assignment(selected).model_dump(mode="json")
    c.step.payload.update(executor_model_override="stale-model", executor_reasoning_effort="low")
    token = await claim(c)
    assert token is not None and c.step.executor_profile_id == selected.id
    policy = c.persist.call_args.kwargs["request"]
    assert policy.required_launch_mode is selected.launch_mode
    assert policy.requires_sandbox is (selected.launch_mode is LaunchMode.CLI)
    assert policy.restrict_to_profile_ids == frozenset({selected.id})
    assert policy.allowed_fallback_ids == () and policy.failed_profile_id is None
    assert c.reserve.call_args.kwargs["profile_id"] == selected.id
    assert c.step.payload["project_model_assignment"] == assignment(selected).model_dump(
        mode="json"
    )
    assert "executor_model_override" not in c.step.payload
    c.blocked.assert_not_awaited()


@pytest.mark.anyio
async def test_changed_pinned_profile_blocks_without_spending(route_call):
    c = route_call
    c.registries = bundle(c.profile.model_copy(update={"model": "changed"}), c.alternate)
    c.registries.projects.get.return_value.allowed_capabilities = frozenset(Capability)
    assert await claim(c) is None
    assert c.blocked.call_args.kwargs["category"] == "project_pin_unresolved"
    c.reserve.assert_not_awaited()


@pytest.mark.anyio
async def test_goal_cap_refusal_preserves_reason_for_the_lead(route_call):
    c = route_call
    c.reserve.side_effect = GoalBudgetExceeded("lifetime owner cost budget exhausted")
    assert await claim(c) is None
    assert c.blocked.call_args.kwargs["category"] == "goal_budget_exhausted"
    assert c.blocked.call_args.kwargs["quota"] is False
    assert "/lead limits cost" in c.blocked.call_args.kwargs["summary"]


@pytest.mark.anyio
async def test_busy_pinned_profile_waits_without_using_alternate(route_call, monkeypatch):
    c = route_call
    monkeypatch.setattr(
        routing,
        "_states",
        AsyncMock(
            return_value={
                c.profile.id: EffectiveProfileState(active_leases=c.profile.concurrency_limit)
            }
        ),
    )
    assert await claim(c) is None
    c.reserve.assert_not_awaited()
    c.blocked.assert_not_awaited()
