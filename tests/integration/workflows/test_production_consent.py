"""Production authority is distinct from managed integration and preview."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from tests.integration.leadership.test_lead_review_regressions import StartModel
from tests.integration.leadership.test_managed_goal_flow import _enable, _factory, _session, _worker
from tests.integration.workflows.test_publication_boundary import request_for
from tests.unit.workflows.test_static_publish import _handler
from vuzol.cli.static_publisher_worker import publisher_handlers
from vuzol.ops.static_publish import measure_static_tree
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import Approval, Step, Task, Worktree
from vuzol.storage.types import ApprovalStatus, IdempotencyClass, StepStatus, TaskStatus
from vuzol.telegram.projections import build_approval_card
from vuzol.workflows.controls import decide_result
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.publication import PublicationDenied, publication_authority
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry
from vuzol.workflows.service import commit_step_outcome

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def fixture(factory, tmp_path, *, managed=False):
    if managed:
        await _enable(factory, await _session(factory))
        await _worker(factory, StartModel()).process_one()
        async with factory() as session:
            task_id = await session.scalar(select(Task.id))
        request = await request_for(factory, task_id)
    else:
        request = await request_for(factory)
    config, _ = _handler(tmp_path)
    runtime = config._runtime
    runtime.settings.project_lead = SimpleNamespace(enabled=managed)
    repository = tmp_path / "repositories/demo"
    async with factory.begin() as session:
        task = await session.get(Task, request.task_id)
        task.project_id = "demo"
        step = await session.get(Step, request.step_id)
        step.step_type = "publish_static"
        step.attempt_count = step.max_attempts = 1
        session.add(
            Worktree(
                task_id=task.id,
                run_id=request.run_id,
                project_id="demo",
                repository_identity_hash="a" * 64,
                base_commit="b" * 40,
                expected_target_head="b" * 40,
                default_branch="main",
                branch="fixture",
                path=str(repository),
                owner="test",
                result_commit="a" * 40,
                retention_until=datetime.now(UTC) + timedelta(days=1),
            )
        )
        session.add(
            Step(
                run_id=request.run_id,
                ordinal=2,
                step_type="build_static",
                status=StepStatus.COMPLETED,
                timeout_seconds=60,
                idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
                result={
                    "status": "built",
                    "source_commit": "a" * 40,
                    "source_directory": ".",
                    "artifact_hash": measure_static_tree(repository).digest,
                },
            )
        )
    # Actual CLI assembly, only external HTTP transport is fake.
    handler = publisher_handlers(factory, runtime, PreviewRuntimeRegistry())["publish_static"]
    handler._probe = AsyncMock(return_value={"status_code": 200})
    return replace(request, step_type="publish_static"), handler


async def approve(factory, request):
    async with factory.begin() as session:
        approval = await session.scalar(select(Approval).where(Approval.step_id == request.step_id))
        task = await session.get(Task, request.task_id)
        await decide_result(session, approval.id, decision="approve", deciding_user_id=task.user_id)
    async with factory.begin() as session:
        lease = await claim_step(
            session,
            owner="publisher-test",
            lease_seconds=60,
            capabilities=frozenset(),
            step_types=frozenset({"publish_static"}),
        )
        assert lease is not None, "asking for consent must not exhaust max_attempts=1"
        await start_step(session, lease)
    return replace(request, lease=lease)


async def test_real_publisher_requires_separate_owner_consent_and_replays_same_artifact(
    postgres_dsn,
    tmp_path,
):
    async with _factory(postgres_dsn) as factory:
        request, handler = await fixture(factory, tmp_path)
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.NEEDS_APPROVAL
        assert not (tmp_path / "sites/demo/current").exists()
        handler._probe.assert_not_awaited()
        # Crash after asking for consent but before committing the step outcome.
        repeated = await handler.execute(request, CancellationContext())
        assert repeated.kind is OutcomeKind.NEEDS_APPROVAL
        async with factory() as session:
            assert len((await session.scalars(select(Approval))).all()) == 1
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, result)
        async with factory() as session:
            card = await build_approval_card(session, request.task_id)
            approval = await session.get(Approval, card.approval_id)
            envelope = (await session.get(Step, request.step_id)).payload["action_envelope"]
            assert "production" in card.html
            assert "https://hryshyn.dev/demo/" in card.html
            assert envelope["artifact_hash"] in card.html
            assert card.buttons == ("approve", "reject")
        with pytest.raises(ValueError, match="owner"):
            async with factory.begin() as session:
                await decide_result(session, approval.id, decision="approve", deciding_user_id=999)
        request = await approve(factory, request)
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.SUCCEEDED, result
        assert (tmp_path / "sites/demo/current/index.html").read_text() == "<h1>Demo</h1>"
        async with factory() as session:
            assert (await session.get(Approval, approval.id)).status is ApprovalStatus.CONSUMED
        # Crash before workflow ack: still RUNNING, exact same effect is idempotent.
        replay = await handler.execute(request, CancellationContext())
        assert replay.kind is OutcomeKind.SUCCEEDED
        assert replay.result["changed"] is False


@pytest.mark.parametrize(
    "change",
    [
        "artifact",
        "destination",
        "expired",
        "revoked",
        "auto_integration",
        "pause",
    ],
)
async def test_approved_production_still_fails_closed_on_drift_or_stop(
    postgres_dsn,
    tmp_path,
    change,
):
    async with _factory(postgres_dsn) as factory:
        request, handler = await fixture(factory, tmp_path)
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.NEEDS_APPROVAL
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, result)
        request = await approve(factory, request)
        async with factory.begin() as session:
            approval = await session.scalar(select(Approval))
            if change == "expired":
                approval.expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "revoked":
                approval.status = ApprovalStatus.REJECTED
            elif change == "auto_integration":
                approval.deciding_user_id = None
            elif change == "pause":
                (await session.get(Task, request.task_id)).status = TaskStatus.PAUSED
            elif change == "artifact":
                build = await session.scalar(select(Step).where(Step.step_type == "build_static"))
                build.result = {**build.result, "artifact_hash": "f" * 64}
        if change == "destination":
            handler._runtime.settings.static_site_root = tmp_path / "other-production"
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.BLOCKED, result
        assert not (tmp_path / "sites/demo/current").exists()
        assert not (tmp_path / "other-production/demo/current").exists()
        handler._probe.assert_not_awaited()


async def test_integration_grant_cannot_publish_production_or_stall_managed_package(
    postgres_dsn,
    tmp_path,
):
    async with _factory(postgres_dsn) as factory:
        request, handler = await fixture(factory, tmp_path, managed=True)
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.SUCCEEDED
        assert result.result == {"status": "skipped", "reason": "managed_integration_only"}
        assert not (tmp_path / "sites/demo/current").exists()
        # The effect boundary itself also rejects direct/bypassed calls.
        with pytest.raises(PublicationDenied, match="managed_integration_only"):
            async with publication_authority(
                factory,
                request,
                CancellationContext(),
                feature_enabled=True,
                production_target={
                    "source_commit": "a" * 40,
                    "artifact_hash": "b" * 64,
                    "destination": str(tmp_path / "sites/demo"),
                    "public_url": "https://example.test/",
                },
            ):
                pytest.fail("integration authority escalated to production")
        with pytest.raises(PublicationDenied, match="publication_target_mismatch"):
            async with publication_authority(
                factory,
                request,
                CancellationContext(),
                feature_enabled=True,
            ):
                pytest.fail("production masqueraded as preview")


@pytest.mark.parametrize("stop", ["revoke", "pause", "takeover"])
async def test_authority_is_rechecked_after_preparation_check(
    postgres_dsn,
    tmp_path,
    stop,
):
    async with _factory(postgres_dsn) as factory:
        request, handler = await fixture(factory, tmp_path)
        result = await handler.execute(request, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, result)
        request = await approve(factory, request)
        real_ensure = handler._ensure_approval

        async def stop_after_check(request, cancellation, target):
            assert await real_ensure(request, cancellation, target)
            async with factory.begin() as session:
                if stop == "revoke":
                    (await session.scalar(select(Approval))).status = ApprovalStatus.REJECTED
                elif stop == "pause":
                    (await session.get(Task, request.task_id)).status = TaskStatus.PAUSED
                else:
                    (await session.get(Step, request.step_id)).lease_generation += 1
            return True

        handler._ensure_approval = stop_after_check
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.BLOCKED
        assert not (tmp_path / "sites/demo/current").exists()
        handler._probe.assert_not_awaited()


async def test_lease_expiring_during_approval_lookup_blocks_activation(
    postgres_dsn,
    tmp_path,
    monkeypatch,
):
    from vuzol.workflows import publication

    async with _factory(postgres_dsn) as factory:
        request, handler = await fixture(factory, tmp_path)
        result = await handler.execute(request, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, result)
        request = await approve(factory, request)
        real_lookup = publication._production_approval

        async def slow_lookup(*args, **kwargs):
            approval = await real_lookup(*args, **kwargs)
            if not kwargs.get("create"):
                # Simulate time spent waiting on the approval row lock.
                monkeypatch.setattr(
                    publication,
                    "datetime",
                    SimpleNamespace(now=lambda tz: datetime.now(tz) + timedelta(minutes=30)),
                )
            return approval

        monkeypatch.setattr(publication, "_production_approval", slow_lookup)
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.BLOCKED
        assert result.category == "publication_lease_lost"
        assert not (tmp_path / "sites/demo/current").exists()
        handler._probe.assert_not_awaited()
