"""PostgreSQL claim-path tests for disk-pressure heavy-work backpressure."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import select

from vuzol.config.settings import DiskPressureSettings, Settings
from vuzol.ops.disk_pressure import DISK_PRESSURE_CATEGORY
from vuzol.storage.leasing import claim_step
from vuzol.storage.models import Step
from vuzol.storage.types import IdempotencyClass, QueueClass, RetryClass, StepStatus
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.service import commit_step_outcome

from .helpers import seed_task_run_step, storage


class _Probe:
    def __init__(self, free: int) -> None:
        self.free = free

    def free_bytes(self, path: Path) -> int:
        del path
        return self.free


def _settings(tmp_path: Path, *, min_free: int) -> Settings:
    return Settings(
        environment="test",
        repository_root=tmp_path / "repositories",
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        secret_file_root=tmp_path / "secrets",
        disk_pressure=DiskPressureSettings(min_free_bytes=min_free),
    )


@pytest.mark.postgresql
def test_heavy_claim_skipped_when_disk_low(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.HEAVY,
        )
        settings = _settings(tmp_path, min_free=10_000)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=settings,
                free_space_probe=_Probe(100),
            )
        assert token is None
        async with factory() as session:
            step = (await session.scalars(select(Step))).one()
            assert step.status is StepStatus.QUEUED
            assert step.attempt_count == 0
            assert step.lease_owner is None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_heavy_claim_allowed_when_disk_ok(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.HEAVY,
        )
        settings = _settings(tmp_path, min_free=10_000)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=settings,
                free_space_probe=_Probe(50_000),
            )
        assert token is not None
        async with factory() as session:
            step = await session.get(Step, token.step.id)
            assert step is not None
            assert step.attempt_count == 1
            assert step.status is StepStatus.LEASED
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_light_claim_not_gated_by_disk_pressure(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.LIGHT,
        )
        settings = _settings(tmp_path, min_free=10_000)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="worker",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.LIGHT}),
                settings=settings,
                free_space_probe=_Probe(0),
            )
        assert token is not None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_disabled_disk_gate_allows_heavy_claim(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.HEAVY,
        )
        settings = _settings(tmp_path, min_free=0)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=settings,
                free_space_probe=_Probe(0),
            )
        assert token is not None
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_heavy_claim_without_settings_fails_closed(postgres_dsn: str, tmp_path: Path) -> None:
    """Missing settings must not silently allow HEAVY claims (C3)."""

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.HEAVY,
        )
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=None,
                free_space_probe=_Probe(10**12),
            )
        assert token is None
        async with factory() as session:
            step = (await session.scalars(select(Step))).one()
            assert step.status is StepStatus.QUEUED
            assert step.attempt_count == 0
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_heavy_claim_recovers_when_probe_reports_space(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.HEAVY,
        )
        settings = _settings(tmp_path, min_free=10_000)
        probe = _Probe(100)
        async with factory.begin() as session:
            blocked = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=settings,
                free_space_probe=probe,
            )
        assert blocked is None
        probe.free = 50_000
        async with factory.begin() as session:
            recovered = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=settings,
                free_space_probe=probe,
            )
        assert recovered is not None
        async with factory() as session:
            step = await session.get(Step, recovered.step.id)
            assert step is not None and step.attempt_count == 1
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.postgresql
def test_disk_pressure_transient_refunds_attempt(postgres_dsn: str, tmp_path: Path) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        # Match coding.prepare_worktree: NEVER retry class + ISOLATED_RETRYABLE.
        await seed_task_run_step(
            factory,
            capabilities=["code_edit"],
            queue_class=QueueClass.HEAVY,
            retry_class=RetryClass.NEVER,
            max_attempts=1,
            idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
            step_type="prepare_worktree",
        )
        settings = _settings(tmp_path, min_free=0)
        async with factory.begin() as session:
            token = await claim_step(
                session,
                owner="executor",
                lease_seconds=60,
                capabilities=frozenset({"code_edit"}),
                queue_classes=frozenset({QueueClass.HEAVY}),
                settings=settings,
            )
        assert token is not None
        async with factory() as session:
            claimed = await session.get(Step, token.step.id)
            assert claimed is not None and claimed.attempt_count == 1
        async with factory.begin() as session:
            await commit_step_outcome(
                session,
                token,
                StepOutcome(
                    kind=OutcomeKind.TRANSIENT_FAILURE,
                    result={},
                    category=DISK_PRESSURE_CATEGORY,
                    summary="insufficient free disk for heavy work",
                ),
                retry_delay_seconds=30,
            )
        async with factory() as session:
            step = (await session.scalars(select(Step))).one()
            assert step.status is StepStatus.QUEUED
            assert step.attempt_count == 0
            assert step.failure_category == DISK_PRESSURE_CATEGORY
        await engine.dispose()

    asyncio.run(scenario())
