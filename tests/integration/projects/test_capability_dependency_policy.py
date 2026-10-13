"""Current policy through existing handlers, approvals and real TEST PostgreSQL."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tarfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from vuzol.config import CapabilityProvisioningSettings, DependencyProvisioningSettings
from vuzol.project_environment import current_environment, environment_hash
from vuzol.projects.capability_provisioning import (
    CAPABILITY_BUNDLE_SCHEMA,
    CapabilityProvisioningHandler,
    OfflineCapabilityInstaller,
)
from vuzol.projects.dependencies import DependencyEnvironment
from vuzol.projects.dependency_provisioning import DependencyProvisioningHandler
from vuzol.projects.installations import installation_states, receipt_digest
from vuzol.projects.toolchains import load_installed_toolchain
from vuzol.storage.errors import LeaseLost
from vuzol.storage.leasing import claim_step, start_step
from vuzol.storage.models import (
    Approval,
    CapabilityInstallation,
    ProjectEnvironmentRevision,
    Step,
    Task,
    Worktree,
)
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import ApprovalStatus, QueueClass, StepStatus
from vuzol.telegram.projections import build_status_card
from vuzol.workflows.controls import cancel_task, decide_result
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.recovery import recover_expired_steps
from vuzol.workflows.retry_policy import current_retry_target
from vuzol.workflows.service import commit_step_outcome

from ..storage.helpers import seed_task_run_step, storage

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize(
    ("probe", "expiry", "expected"),
    (
        ("failed", True, "failed"),
        ("unknown", True, "failed"),
        ("healthy", False, "stale"),
    ),
)
def test_inventory_installed_label_alone_is_not_health(
    postgres_dsn: str,
    tmp_path: Path,
    probe: str,
    expiry: bool,
    expected: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        async with factory.begin() as session:
            session.add(
                CapabilityInstallation(
                    capability_key="node-runtime",
                    node_id="local",
                    version="test-1",
                    installation_root=str(tmp_path),
                    status="installed",
                    probe_status=probe,
                    health_until=(datetime.now(UTC) + timedelta(hours=1)) if expiry else None,
                )
            )
        async with factory() as session:
            assert (await installation_states(session))["node-runtime"] == expected
        await engine.dispose()

    asyncio.run(scenario())


def test_unregistered_partial_installation_needs_attention_not_another_install_approval(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        installer = _installer(tmp_path)
        (installer.installation_root / "node-runtime").mkdir(parents=True)
        request = await _seed(factory, "partial")
        outcome = await CapabilityProvisioningHandler(factory, installer).execute(
            request,
            CancellationContext(),
        )
        assert outcome.kind is OutcomeKind.BLOCKED
        assert "incomplete" in str(outcome.summary)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Approval)) == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ("outcome", "lease"))
def test_failed_installation_leader_does_not_strand_shared_waiters(
    postgres_dsn: str,
    tmp_path: Path,
    failure: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        installer = _installer(tmp_path)
        handler = CapabilityProvisioningHandler(factory, installer)
        leader = await _seed(factory, "failed-leader")
        first = await handler.execute(leader, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, leader.lease, first)
        waiter = await _seed(factory, "failed-waiter")
        waiting = await handler.execute(waiter, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, waiter.lease, waiting)
        await _approve(factory, first.result["approval_id"])
        leader = await _lease(factory, leader)
        if failure == "outcome":
            async with factory.begin() as session:
                await commit_step_outcome(
                    session,
                    leader.lease,
                    StepOutcome(
                        kind=OutcomeKind.BLOCKED,
                        result={},
                        category="capability_provisioning_failed",
                        unknown_effects=True,
                    ),
                )
        else:
            async with factory.begin() as session:
                row = await session.get(Step, leader.step_id)
                assert row is not None
                row.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            async with factory.begin() as session:
                assert await recover_expired_steps(session, batch_size=10) == 1
        async with factory() as session:
            row = await session.get(Step, waiter.step_id)
            assert row is not None and row.status is StepStatus.QUEUED
            approval = await session.scalar(select(Approval))
            assert approval is not None and approval.status is ApprovalStatus.EXPIRED
        waiter = await _lease(factory, waiter)
        assert (await handler.execute(waiter, CancellationContext())).kind is OutcomeKind.BLOCKED
        assert not installer.ready("node-runtime")
        await engine.dispose()

    asyncio.run(scenario())


def test_blocked_setup_has_usable_same_task_retry_controls(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        parent = await _seed(factory, "disabled-setup", step_type="ensure_dependencies")
        handler = CapabilityProvisioningHandler(factory, _installer(tmp_path, enabled=False))
        needed = await handler.ensure_before_dependencies(parent, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, parent.lease, needed)
        preflight = await _claim_applier(factory, parent)
        blocked = await handler.execute(preflight, CancellationContext())
        assert blocked.category == "capability_setup_required"
        async with factory.begin() as session:
            await commit_step_outcome(session, preflight.lease, blocked)
        async with factory() as session:
            steps = tuple(
                (
                    await session.scalars(
                        select(Step).where(
                            Step.run_id == parent.run_id,
                        )
                    )
                ).all()
            )
            target = current_retry_target(steps)
            assert target is not None and target.id == preflight.step_id
            card = await build_status_card(session, parent.task_id)
            assert any("v2:retry:" in data for row in card.callback_buttons for _text, data in row)
        await engine.dispose()

    asyncio.run(scenario())


def test_capability_handoff_is_atomic_with_outcome_and_cancel_fenced(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        request = await _seed(factory, "atomic", step_type="ensure_dependencies")
        handler = CapabilityProvisioningHandler(factory, _installer(tmp_path))
        outcome = await handler.ensure_before_dependencies(request, CancellationContext())
        assert outcome.category == "capability_preflight_required"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Step)) == 1
        with pytest.raises(RuntimeError, match="test crash"):
            async with factory.begin() as session:
                await commit_step_outcome(session, request.lease, outcome)
                raise RuntimeError("test crash before outcome transaction commits")
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Step)) == 1
            row = await session.get(Step, request.step_id)
            assert row is not None and "capability_preflight_step_id" not in row.payload
        async with factory.begin() as session:
            await cancel_task(session, request.task_id, actor_id="1")
        with pytest.raises(LeaseLost):
            async with factory.begin() as session:
                await commit_step_outcome(session, request.lease, outcome)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Step)) == 1
            assert await session.scalar(select(func.count()).select_from(Approval)) == 0
        await engine.dispose()

    asyncio.run(scenario())


def test_cancelled_installation_leader_releases_other_project_waiters(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        handler = CapabilityProvisioningHandler(factory, _installer(tmp_path))
        leader = await _seed(factory, "cancel-leader")
        first = await handler.execute(leader, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, leader.lease, first)
        waiter = await _seed(factory, "cancel-waiter")
        waiting = await handler.execute(waiter, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, waiter.lease, waiting)
        async with factory.begin() as session:
            await cancel_task(session, leader.task_id, actor_id="1")
        async with factory() as session:
            row = await session.get(Step, waiter.step_id)
            assert row is not None and row.status is StepStatus.QUEUED
            approval = await session.scalar(select(Approval))
            assert approval is not None and approval.status is ApprovalStatus.EXPIRED
        waiter = await _lease(factory, waiter)
        assert (await handler.execute(waiter, CancellationContext())).kind is OutcomeKind.BLOCKED
        await engine.dispose()

    asyncio.run(scenario())


def test_project_local_dependencies_build_without_creating_an_approval(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        request = await _seed(
            factory, "local-deps", keys=("python-runtime",), step_type="ensure_dependencies"
        )
        root = tmp_path / "worktrees"
        path = root / "local-deps"
        path.mkdir(parents=True)
        (path / "pyproject.toml").write_text(
            '[project]\nname="local-deps"\ndependencies=["httpx==0.28.1"]\n'
        )
        async with factory.begin() as session:
            session.add(
                Worktree(
                    task_id=request.task_id,
                    run_id=request.run_id,
                    project_id="local-deps",
                    repository_identity_hash="a" * 64,
                    base_commit="b" * 40,
                    default_branch="main",
                    expected_target_head="b" * 40,
                    branch="test-local-deps",
                    path=str(path),
                    owner="policy-test",
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        builder = AsyncMock()

        async def build(*_args: Any, **kwargs: Any) -> DependencyEnvironment:
            dependency = kwargs["request"]
            assert kwargs["project_id"] == "local-deps"
            assert kwargs["worktree"] == path
            assert dependency.direct_dependencies == ("httpx==0.28.1",)
            return DependencyEnvironment(
                dependency, tmp_path / "environments" / "local-deps", "uv.lock", "c" * 64
            )

        builder.build.side_effect = build
        handler = DependencyProvisioningHandler(
            factory,
            DependencyProvisioningSettings(
                enabled=True, environment_root=tmp_path / "environments"
            ),
            builder,
            worktree_root=root,
            capabilities=CapabilityProvisioningHandler(
                factory,
                _installer(tmp_path, enabled=False),
                worktree_root=root,
            ),
        )
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.kind is OutcomeKind.SUCCEEDED
        assert outcome.result["authorization"] == "project_local_dependency_policy"
        builder.build.assert_awaited_once()
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Approval)) == 0
        await engine.dispose()

    asyncio.run(scenario())


def _installer(tmp_path: Path, *, enabled: bool = True) -> OfflineCapabilityInstaller:
    bundles = tmp_path / "bundles"
    bundles.mkdir(parents=True, exist_ok=True)
    for key, command in (("node-runtime", "node"), ("java-runtime", "java")):
        archive = bundles / f"{key}.tar"
        content = b"#!/bin/sh\nexit 0\n"
        with tarfile.open(archive, "w") as output:
            info = tarfile.TarInfo(f"bin/{command}")
            info.size = len(content)
            info.mode = 0o755
            output.addfile(info, io.BytesIO(content))
        archive.chmod(0o644)
        manifest = bundles / f"{key}.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": CAPABILITY_BUNDLE_SCHEMA,
                    "capability_key": key,
                    "version": "test-1",
                    "archive": archive.name,
                    "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                    "executables": {command: f"bin/{command}"},
                }
            )
        )
        manifest.chmod(0o644)
    return OfflineCapabilityInstaller(
        CapabilityProvisioningSettings(
            enabled=enabled,
            bundle_root=bundles,
            toolchain_root=tmp_path / "toolchains",
        )
    )


async def _lease(factory: Any, request: StepExecutionRequest) -> StepExecutionRequest:
    expires = datetime.now(UTC) + timedelta(hours=1)
    async with factory.begin() as session:
        step = await session.get(Step, request.step_id)
        assert step is not None
        step.status = StepStatus.RUNNING
        step.lease_owner = "policy-test"
        step.lease_generation += 1
        step.lease_expires_at = expires
        step.attempt_count = 1
        record = StepRecord(
            step.id, step.run_id, step.status, step.lease_generation, step.lease_owner, expires
        )
        return replace(request, lease=LeaseToken(record, "policy-test", step.lease_generation))


async def _seed(
    factory: Any,
    project: str,
    *,
    keys: tuple[str, ...] = ("node-runtime",),
    step_type: str = "ensure_capabilities",
) -> StepExecutionRequest:
    task, run_id, step = await seed_task_run_step(
        factory,
        step_status=StepStatus.RUNNING,
        step_type=step_type,
        queue_class=QueueClass.PRIVILEGED,
        max_attempts=1,
    )
    contract: dict[str, object] = {
        "components": {},
        "capabilities": {
            key: {"key": key, "label": key, "provisioning": "approval_required"} for key in keys
        },
    }
    async with factory.begin() as session:
        row = await session.get(Task, task.id)
        assert row is not None
        row.project_id = project
        session.add(
            ProjectEnvironmentRevision(
                project_id=project,
                revision_number=1,
                source="detected",
                contract=contract,
                content_hash=environment_hash(contract),
            )
        )
    request = StepExecutionRequest(
        task.id, run_id, step.id, step_type, {}, 1800, LeaseToken(step, "policy-test", 0)
    )
    return await _lease(factory, request)


async def _approve(factory: Any, approval_id: str) -> None:
    import uuid

    async with factory.begin() as session:
        await decide_result(session, uuid.UUID(approval_id), decision="approve", deciding_user_id=1)


async def _claim_applier(factory: Any, parent: StepExecutionRequest) -> StepExecutionRequest:
    async with factory.begin() as session:
        token = await claim_step(
            session,
            owner="applier-policy-test",
            lease_seconds=60,
            capabilities=frozenset({"host_admin"}),
            queue_classes=frozenset({QueueClass.PRIVILEGED}),
            step_types=frozenset({"ensure_capabilities"}),
        )
        assert token is not None
        await start_step(session, token)
        row = await session.get(Step, token.step.id)
        assert row is not None
        return StepExecutionRequest(
            parent.task_id, parent.run_id, row.id, row.step_type, row.payload, 1800, token
        )


def test_concurrent_projects_share_one_install_and_reuse_without_approval(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        installer = _installer(tmp_path)
        handler = CapabilityProvisioningHandler(factory, installer)
        requests = [await _seed(factory, "first"), await _seed(factory, "second")]
        outcomes = await asyncio.gather(
            *(handler.execute(request, CancellationContext()) for request in requests)
        )
        assert all(outcome.kind is OutcomeKind.NEEDS_APPROVAL for outcome in outcomes)
        assert outcomes[0].result["approval_id"] == outcomes[1].result["approval_id"]
        async with factory() as session:
            approval = await session.scalar(select(Approval))
            assert approval is not None
            leader_index = next(
                i for i, request in enumerate(requests) if request.step_id == approval.step_id
            )
        leader = requests[leader_index]
        waiter = requests[1 - leader_index]
        for request, outcome in zip(requests, outcomes, strict=True):
            async with factory.begin() as session:
                await commit_step_outcome(session, request.lease, outcome)
        async with factory() as session:
            card = await build_status_card(session, waiter.task_id)
            assert "повторный апрув не нужен" in card.html
            assert card.approval_id is None
        await _approve(factory, outcomes[0].result["approval_id"])
        leader = await _lease(factory, leader)
        installed = await handler.execute(leader, CancellationContext())
        assert installed.kind is OutcomeKind.SUCCEEDED
        assert installed.result["status"] == "installed"
        async with factory() as session:
            row = await session.get(Step, waiter.step_id)
            assert row is not None and row.status is StepStatus.QUEUED
        waiter = await _lease(factory, waiter)
        reused = await handler.execute(waiter, CancellationContext())
        assert reused.kind is OutcomeKind.SUCCEEDED
        assert reused.result["status"] == "ready"
        third = await _seed(factory, "third")
        disabled_handler = CapabilityProvisioningHandler(
            factory, _installer(tmp_path, enabled=False)
        )
        assert (
            await disabled_handler.execute(third, CancellationContext())
        ).kind is OutcomeKind.SUCCEEDED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Approval)) == 1
            installation = await session.scalar(select(CapabilityInstallation))
            assert installation is not None and installation.probe_status == "healthy"
            assert installation.node_id == "local"
        await engine.dispose()

    asyncio.run(scenario())


def test_overlapping_requirements_do_not_request_node_install_twice(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        handler = CapabilityProvisioningHandler(factory, _installer(tmp_path))
        leader = await _seed(factory, "node-only")
        first = await handler.execute(leader, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, leader.lease, first)
        waiter = await _seed(factory, "node-and-java", keys=("node-runtime", "java-runtime"))
        waiting = await handler.execute(waiter, CancellationContext())
        assert waiting.result["approval_id"] == first.result["approval_id"]
        async with factory.begin() as session:
            await commit_step_outcome(session, waiter.lease, waiting)
        await _approve(factory, first.result["approval_id"])
        leader = await _lease(factory, leader)
        assert (await handler.execute(leader, CancellationContext())).kind is OutcomeKind.SUCCEEDED
        waiter = await _lease(factory, waiter)
        java_approval = await handler.execute(waiter, CancellationContext())
        assert java_approval.kind is OutcomeKind.NEEDS_APPROVAL
        assert java_approval.result["capabilities"] == ["java-runtime"]
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Approval)) == 2
        await engine.dispose()

    asyncio.run(scenario())


def test_rejection_wakes_shared_waiter_without_installing(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        import uuid

        engine, factory = storage(postgres_dsn)
        installer = _installer(tmp_path)
        handler = CapabilityProvisioningHandler(factory, installer)
        leader = await _seed(factory, "reject-leader")
        first = await handler.execute(leader, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, leader.lease, first)
        waiter = await _seed(factory, "reject-waiter")
        waiting = await handler.execute(waiter, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, waiter.lease, waiting)
        async with factory.begin() as session:
            await decide_result(
                session,
                uuid.UUID(first.result["approval_id"]),
                decision="reject",
                deciding_user_id=1,
            )
        async with factory() as session:
            row = await session.get(Step, waiter.step_id)
            assert row is not None and row.status is StepStatus.QUEUED
        waiter = await _lease(factory, waiter)
        result = await handler.execute(waiter, CancellationContext())
        assert result.kind is OutcomeKind.BLOCKED
        assert not installer.ready("node-runtime")
        async with factory() as session:
            approval = await session.scalar(select(Approval))
            assert approval is not None and approval.status is ApprovalStatus.REJECTED
        await engine.dispose()

    asyncio.run(scenario())


def test_shared_install_settling_before_waiter_outcome_does_not_lose_wakeup(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        handler = CapabilityProvisioningHandler(factory, _installer(tmp_path))
        leader = await _seed(factory, "leader")
        first = await handler.execute(leader, CancellationContext())
        async with factory.begin() as session:
            await commit_step_outcome(session, leader.lease, first)
        waiter = await _seed(factory, "waiter")
        waiting = await handler.execute(waiter, CancellationContext())
        await _approve(factory, first.result["approval_id"])
        leader = await _lease(factory, leader)
        assert (await handler.execute(leader, CancellationContext())).kind is OutcomeKind.SUCCEEDED
        async with factory.begin() as session:
            await commit_step_outcome(session, waiter.lease, waiting)
        async with factory() as session:
            row = await session.get(Step, waiter.step_id)
            assert row is not None and row.status is StepStatus.QUEUED and row.attempt_count == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("health", ("stale", "failed", "changed", "missing"))
def test_installed_is_not_healthy_and_stale_can_be_reverified_without_approval(
    postgres_dsn: str,
    tmp_path: Path,
    health: str,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        installer = _installer(tmp_path)
        installer.install(installer.inspect_bundle("node-runtime"))
        handler = CapabilityProvisioningHandler(factory, installer)
        request = await _seed(factory, "health")
        assert (await handler.execute(request, CancellationContext())).kind is OutcomeKind.SUCCEEDED
        async with factory.begin() as session:
            row = await session.scalar(select(CapabilityInstallation))
            assert row is not None
            row.health_until = datetime.now(UTC) - timedelta(seconds=1)
            if health == "failed":
                row.status = "failed"
                row.probe_status = "failed"
            elif health == "changed":
                row.receipt_hash = "f" * 64
            elif health == "missing":
                executable = installer.installation_root / "node-runtime/bin/node"
                executable.chmod(0o644)
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.kind is (OutcomeKind.SUCCEEDED if health == "stale" else OutcomeKind.BLOCKED)
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Approval)) == 0
            row = await session.scalar(select(CapabilityInstallation))
            assert row is not None
            if health == "stale":
                assert row.health_until is not None
                assert row.health_until > datetime.now(UTC)
                spec = load_installed_toolchain(installer.installation_root, "node-runtime")
                assert spec is not None and row.receipt_hash == receipt_digest(spec)
        await engine.dispose()

    asyncio.run(scenario())


def test_generated_makefile_discovers_node_before_validation_even_without_dependencies(
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        request = await _seed(factory, "generated", keys=(), step_type="ensure_dependencies")
        root = tmp_path / "worktrees"
        path = root / "generated"
        path.mkdir(parents=True)
        (path / "Makefile").write_text("test:\n\tnode --check app.js\n\tnode tests/app.test.js\n")
        async with factory.begin() as session:
            session.add(
                Worktree(
                    task_id=request.task_id,
                    run_id=request.run_id,
                    project_id="generated",
                    repository_identity_hash="a" * 64,
                    base_commit="b" * 40,
                    default_branch="main",
                    expected_target_head="b" * 40,
                    branch="test-generated",
                    path=str(path),
                    owner="policy-test",
                    retention_until=datetime.now(UTC) + timedelta(days=1),
                )
            )
        capability_handler = CapabilityProvisioningHandler(
            factory,
            _installer(tmp_path),
            worktree_root=root,
        )
        handler = DependencyProvisioningHandler(
            factory,
            DependencyProvisioningSettings(enabled=False),
            None,
            worktree_root=root,
            capabilities=capability_handler,
        )
        outcome = await handler.execute(request, CancellationContext())
        assert outcome.kind is OutcomeKind.BLOCKED
        assert outcome.category == "capability_preflight_required"
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(Approval)) == 0
        async with factory.begin() as session:
            await commit_step_outcome(session, request.lease, outcome)
        preflight = await _claim_applier(factory, request)
        approval_outcome = await capability_handler.execute(preflight, CancellationContext())
        assert approval_outcome.kind is OutcomeKind.NEEDS_APPROVAL
        async with factory.begin() as session:
            await commit_step_outcome(session, preflight.lease, approval_outcome)
        await _approve(factory, approval_outcome.result["approval_id"])
        # Normal scheduler, not a manual attempt reset: max_attempts=1 still works.
        preflight = await _claim_applier(factory, request)
        installed = await capability_handler.execute(preflight, CancellationContext())
        assert installed.kind is OutcomeKind.SUCCEEDED
        async with factory.begin() as session:
            await commit_step_outcome(session, preflight.lease, installed)
        async with factory() as session:
            waiting = await session.get(Step, request.step_id)
            assert waiting is not None and waiting.status is StepStatus.QUEUED
            assert waiting.attempt_count == 0 and waiting.max_attempts == 1
        request = await _lease(factory, request)
        result = await handler.execute(request, CancellationContext())
        assert result.kind is OutcomeKind.SUCCEEDED and result.result["status"] == "skipped"
        async with factory() as session:
            environment = await current_environment(session, "generated")
            assert environment is not None and environment.revision_number == 2
            assert "node-runtime" in environment.contract["capabilities"]
            assert environment.contract["components"] == {}
            assert environment.approved_by_user_id is None
            assert await session.scalar(select(func.count()).select_from(Approval)) == 1
        await engine.dispose()

    asyncio.run(scenario())
