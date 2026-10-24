"""CLI review authorization and fenced account/worktree handoff, without a database."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config.models import Capability, LaunchMode, ProviderRole
from vuzol.config.revision import content_revision
from vuzol.providers.cli_readonly import ReadOnlyCliAdapter
from vuzol.providers.domain import ProviderErrorCategory
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.kimi import KIMI_MODEL
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import Run, Step, Task, Worktree
from vuzol.storage.types import RunStatus, StepStatus, WorktreeDeliveryState
from vuzol.workflows.ports import CancellationContext

from ._test_providers_helpers import profile, provider_request


@pytest.fixture(params=("codex", "grok", "kimi", "pi"))
def review_call(request, tmp_path):
    configured = profile(
        "review-account",
        provider=request.param,
        model=KIMI_MODEL if request.param == "kimi" else "another-vendor/model",
        model_reasoning_effort="high",
        launch_mode=LaunchMode.CLI,
        api_base_url=None,
        roles={ProviderRole.REVIEWER, ProviderRole.EXECUTOR},
        runtime_identity="review-account",
        state_directory=tmp_path / "state",
    )
    call = provider_request().model_copy(
        update={
            "role": ProviderRole.REVIEWER,
            "lease_generation": 3,
            "required_capabilities": frozenset({Capability.CODE_EDIT}),
        }
    )
    step = Step(
        id=call.step_id,
        run_id=call.run_id,
        step_type="review",
        status=StepStatus.RUNNING,
        lease_generation=3,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=10),
        payload={"retained": "payload"},
    )
    task = Task(id=call.task_id, project_id="project")
    run = Run(id=call.run_id, task_id=call.task_id, status=RunStatus.RUNNING)
    worktree = Worktree(
        id=uuid.uuid4(),
        task_id=call.task_id,
        run_id=call.run_id,
        project_id="project",
        path=str(tmp_path / "worktree"),
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        result_commit="b" * 40,
    )
    rows = {Step: step, Task: task, Run: run, Worktree: worktree}
    session = AsyncMock()
    session.get.side_effect = lambda model, _id, **_kwargs: rows[model]
    session.scalar.side_effect = [0, worktree]
    factory = MagicMock()
    factory.begin.return_value.__aenter__.return_value = session
    runtime = MagicMock()
    runtime.settings.worktree_root = tmp_path / "worktrees"
    runtime.settings.execution.rootless_docker_socket = tmp_path / "docker.sock"
    runtime.registries.projects.get.return_value.sandbox_profile = "sandbox"
    runtime.registries.projects.get.return_value.allowed_capabilities = frozenset(
        {Capability.REPOSITORY_READ}
    )
    runtime.registries.sandboxes.get.return_value = SimpleNamespace(uid=1000, gid=1000)
    adapter = MagicMock(execute=AsyncMock())
    wrapper = ReadOnlyCliAdapter(adapter, runtime, factory)
    access = MagicMock(revoke=AsyncMock())
    wrapper._access = MagicMock(grant=AsyncMock(return_value=access))
    return SimpleNamespace(
        wrapper=wrapper,
        runtime=runtime,
        adapter=adapter,
        access=access,
        factory=factory,
        session=session,
        rows=rows,
        request=call,
        profile=configured,
        step=step,
        run=run,
        task=task,
        worktree=worktree,
        cancellation=CancellationContext(),
    )


@pytest.mark.anyio
async def test_review_pins_profile_and_bounds_capabilities_without_replacing_identity(review_call):
    c = review_call
    result = await c.wrapper.execute(c.request, c.profile, c.cancellation)

    assert result is c.adapter.execute.return_value
    bounded, configured, cancellation = c.adapter.execute.call_args.args
    assert bounded == c.request.model_copy(
        update={
            "sandbox_reference": f"worktree:{c.worktree.id}",
            "required_capabilities": frozenset({Capability.REPOSITORY_READ}),
        }
    )
    assert configured is c.profile and cancellation is c.cancellation
    assert c.step.executor_profile_id == c.profile.id
    assert c.step.payload == {
        "retained": "payload",
        "cli_reviewer_pin": {
            "profile_id": c.profile.id,
            "profile_revision": content_revision(c.profile),
        },
        "executor_model_override": c.profile.model,
        "executor_reasoning_effort": "high",
    }
    c.session.get.assert_any_await(Step, c.request.step_id, with_for_update=True)
    c.wrapper._access.grant.assert_awaited_once_with(
        c.profile.state_directory.parent / "worktree", sandbox_uid=1000, sandbox_gid=1000
    )
    c.access.revoke.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "role",
    [role for role in ProviderRole if role is not ProviderRole.REVIEWER],
)
async def test_non_reviewer_requests_fail_before_claim_or_launch(review_call, role):
    c = review_call
    with pytest.raises(ValueError, match="reviewer required"):
        await c.wrapper.execute(
            c.request.model_copy(update={"role": role}), c.profile, c.cancellation
        )
    c.factory.begin.assert_not_called()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "changes",
    [
        {"enabled": False},
        {"provider": "unsupported"},
        {"launch_mode": LaunchMode.API},
        {"roles": frozenset({ProviderRole.EXECUTOR})},
    ],
)
async def test_ineligible_profile_fails_before_claim_or_launch(review_call, changes):
    c = review_call
    with pytest.raises(ValueError, match="supported CLI role profile"):
        await c.wrapper.execute(c.request, c.profile.model_copy(update=changes), c.cancellation)
    c.factory.begin.assert_not_called()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("row", "field", "value"),
    [
        ("step", "lease_generation", 4),
        ("step", "lease_expires_at", datetime(2000, 1, 1, tzinfo=UTC)),
        ("step", "lease_expires_at", None),
        ("step", "status", StepStatus.COMPLETED),
        ("step", "step_type", "execute_agent"),
        ("step", "run_id", uuid.uuid4()),
        ("run", "task_id", uuid.uuid4()),
        ("run", "status", RunStatus.COMPLETED),
        ("task", "project_id", None),
    ],
)
async def test_stale_or_mismatched_claim_never_grants_access(review_call, row, field, value):
    c = review_call
    setattr(getattr(c, row), field, value)
    with pytest.raises(LeaseLost, match="stale"):
        await c.wrapper.execute(c.request, c.profile, c.cancellation)
    c.wrapper._access.grant.assert_not_awaited()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
async def test_changed_profile_pin_fails_before_account_claim(review_call):
    c = review_call
    c.step.payload["cli_reviewer_pin"] = {
        "profile_id": c.profile.id,
        "profile_revision": "previous-revision",
    }
    with pytest.raises(ProviderFailure, match="assignment changed") as failure:
        await c.wrapper.execute(c.request, c.profile, c.cancellation)
    assert failure.value.category is ProviderErrorCategory.PERMANENT_REQUEST
    assert not failure.value.request_sent and not failure.value.retryable
    c.session.scalar.assert_not_awaited()
    c.wrapper._access.grant.assert_not_awaited()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("boundary", ["project", "profile"])
async def test_missing_repository_permission_never_grants_access(review_call, boundary):
    c = review_call
    configured = c.profile
    if boundary == "project":
        c.runtime.registries.projects.get.return_value.allowed_capabilities = frozenset()
    else:
        configured = configured.model_copy(update={"capabilities": frozenset()})
    with pytest.raises(ProviderFailure, match="repository read permission") as failure:
        await c.wrapper.execute(c.request, configured, c.cancellation)
    assert not failure.value.request_sent
    c.session.scalar.assert_not_awaited()
    c.wrapper._access.grant.assert_not_awaited()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
async def test_busy_profile_fails_without_launch_or_fallback(review_call):
    c = review_call
    c.session.scalar.side_effect = [c.profile.concurrency_limit]
    with pytest.raises(ProviderFailure, match="busy") as failure:
        await c.wrapper.execute(c.request, c.profile, c.cancellation)
    assert failure.value.category is ProviderErrorCategory.PROVIDER_UNAVAILABLE
    assert failure.value.retryable and not failure.value.request_sent
    c.wrapper._access.grant.assert_not_awaited()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("project_id", "other-project"),
        ("delivery_state", WorktreeDeliveryState.ACTIVE),
        ("result_commit", None),
    ],
)
async def test_unretained_worktree_never_grants_access(review_call, field, value):
    c = review_call
    setattr(c.worktree, field, value)
    with pytest.raises(LeaseLost, match="retained worktree"):
        await c.wrapper.execute(c.request, c.profile, c.cancellation)
    c.wrapper._access.grant.assert_not_awaited()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
async def test_cancelled_review_never_claims_or_launches(review_call):
    c = review_call
    c.cancellation.request()
    with pytest.raises(LeaseLost, match="cancelled"):
        await c.wrapper.execute(c.request, c.profile, c.cancellation)
    c.factory.begin.assert_not_called()
    c.wrapper._access.grant.assert_not_awaited()
    c.adapter.execute.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [RuntimeError("transport failed"), asyncio.CancelledError()])
async def test_transport_failure_or_cancellation_revokes_access(review_call, failure):
    c = review_call
    c.adapter.execute.side_effect = failure
    with pytest.raises(type(failure)):
        await c.wrapper.execute(c.request, c.profile, c.cancellation)
    c.access.revoke.assert_awaited_once()
