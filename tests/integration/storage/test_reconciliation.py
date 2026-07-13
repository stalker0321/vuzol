import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.execution.proxy_service import (
    ProxyRecoveryManifest,
    ProxyServiceError,
    ProxyServiceManager,
)
from vuzol.execution.reconciliation import (
    RECONCILIATION_LOCK_KEY,
    ProxyStartupReconciler,
    ReconciliationClassification,
)
from vuzol.storage.models import Event, Step
from vuzol.storage.types import StepStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


class RecordingProxyManager(ProxyServiceManager):
    def __init__(self, manifests: list[ProxyRecoveryManifest]) -> None:
        self.manifests = manifests
        self.validated: list[ProxyRecoveryManifest] = []
        self.cleaned: list[ProxyRecoveryManifest] = []
        self.cleanup_started = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.cleanup_release.set()
        self.validation_error: ProxyServiceError | None = None

    def recovery_manifests(self) -> tuple[ProxyRecoveryManifest, ...]:
        return tuple(self.manifests)

    async def validate_recovery_resources(self, manifest: ProxyRecoveryManifest) -> None:
        self.validated.append(manifest)
        if self.validation_error is not None:
            raise self.validation_error

    async def cleanup_recovery_manifest(self, manifest: ProxyRecoveryManifest) -> None:
        self.cleanup_started.set()
        await self.cleanup_release.wait()
        self.cleaned.append(manifest)
        if manifest in self.manifests:
            self.manifests.remove(manifest)


def _manifest(
    task_id: uuid.UUID, run_id: uuid.UUID, step_id: uuid.UUID, generation: int
) -> ProxyRecoveryManifest:
    return ProxyRecoveryManifest(
        directory=Path(f"/safe/{step_id}/{generation}"),
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        lease_generation=generation,
        policy_hash="a" * 64,
    )


async def _set_lease(
    factory: async_sessionmaker[AsyncSession],
    step_id: uuid.UUID,
    *,
    status: StepStatus,
    generation: int,
    expires_at: datetime | None,
) -> None:
    async with factory.begin() as session:
        step = await session.get(Step, step_id)
        assert step is not None
        step.status = status
        step.lease_generation = generation
        step.lease_owner = (
            "executor-a" if status in {StepStatus.LEASED, StepStatus.RUNNING} else None
        )
        step.lease_expires_at = expires_at
        step.heartbeat_at = datetime.now(UTC)


async def test_executor_b_preserves_executor_a_active_matching_lease(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        task, run_id, step = await seed_task_run_step(factory)
        await _set_lease(
            factory,
            step.id,
            status=StepStatus.RUNNING,
            generation=3,
            expires_at=datetime.now(UTC) + timedelta(minutes=2),
        )
        manifest = _manifest(task.id, run_id, step.id, 3)
        manager = RecordingProxyManager([manifest])

        report = await ProxyStartupReconciler(
            factory, manager, owner="executor-b"
        ).reconcile_startup()

        assert report.lock_acquired is True
        assert (
            report.decisions[0].classification
            is ReconciliationClassification.PRESERVED_ACTIVE_LEASE
        )
        assert manager.validated == []
        assert manager.cleaned == []
        async with factory() as session:
            event = await session.scalar(
                select(Event).where(Event.event_type == "execution.startup_reconciliation")
            )
            assert event is not None
            assert event.payload["classification"] == "PRESERVED_ACTIVE_LEASE"
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("status", "generation", "manifest_generation", "expires_delta", "classification"),
    [
        (StepStatus.COMPLETED, 2, 2, None, ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER),
        (StepStatus.RUNNING, 2, 2, -10, ReconciliationClassification.REMOVED_EXPIRED_LEASE),
        (StepStatus.RUNNING, 3, 2, 60, ReconciliationClassification.REMOVED_OLD_GENERATION),
    ],
)
async def test_proven_stale_resource_is_cleaned_exactly(
    postgres_dsn: str,
    status: StepStatus,
    generation: int,
    manifest_generation: int,
    expires_delta: int | None,
    classification: ReconciliationClassification,
) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        task, run_id, step = await seed_task_run_step(factory)
        expires_at = (
            datetime.now(UTC) + timedelta(seconds=expires_delta)
            if expires_delta is not None
            else None
        )
        await _set_lease(
            factory,
            step.id,
            status=status,
            generation=generation,
            expires_at=expires_at,
        )
        manifest = _manifest(task.id, run_id, step.id, manifest_generation)
        manager = RecordingProxyManager([manifest])

        report = await ProxyStartupReconciler(
            factory, manager, owner="executor-b"
        ).reconcile_startup()

        assert report.decisions[0].classification is classification
        assert manager.validated == [manifest]
        assert manager.cleaned == [manifest]
        assert manager.manifests == []
    finally:
        await engine.dispose()


async def test_missing_state_and_foreign_resources_are_preserved(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        missing = _manifest(uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), 1)
        manager = RecordingProxyManager([missing])
        report = await ProxyStartupReconciler(
            factory, manager, owner="executor-b"
        ).reconcile_startup()
        assert (
            report.decisions[0].classification is ReconciliationClassification.PRESERVED_AMBIGUOUS
        )
        assert manager.cleaned == []

        task, run_id, step = await seed_task_run_step(factory)
        await _set_lease(
            factory,
            step.id,
            status=StepStatus.COMPLETED,
            generation=1,
            expires_at=None,
        )
        foreign = _manifest(task.id, run_id, step.id, 1)
        manager = RecordingProxyManager([foreign])
        manager.validation_error = ProxyServiceError("refusing foreign recovery network")
        report = await ProxyStartupReconciler(
            factory, manager, owner="executor-b"
        ).reconcile_startup()
        assert report.decisions[0].classification is ReconciliationClassification.PRESERVED_FOREIGN
        assert manager.cleaned == []
    finally:
        await engine.dispose()


async def test_lock_timeout_performs_no_discovery_or_cleanup(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    holder = factory()
    try:
        await holder.execute(
            text("SELECT pg_advisory_lock(:key)"), {"key": RECONCILIATION_LOCK_KEY}
        )
        manager = RecordingProxyManager([])

        report = await ProxyStartupReconciler(
            factory,
            manager,
            owner="executor-b",
            lock_timeout_seconds=0,
        ).reconcile_startup()

        assert report.lock_acquired is False
        assert manager.validated == []
        assert manager.cleaned == []
    finally:
        await holder.execute(
            text("SELECT pg_advisory_unlock(:key)"), {"key": RECONCILIATION_LOCK_KEY}
        )
        await holder.close()
        await engine.dispose()


async def test_simultaneous_reconcilers_serialize_and_cleanup_once(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    try:
        task, run_id, step = await seed_task_run_step(factory)
        await _set_lease(
            factory,
            step.id,
            status=StepStatus.COMPLETED,
            generation=1,
            expires_at=None,
        )
        manifest = _manifest(task.id, run_id, step.id, 1)
        manager = RecordingProxyManager([manifest])
        manager.cleanup_release.clear()
        first = asyncio.create_task(
            ProxyStartupReconciler(factory, manager, owner="executor-a").reconcile_startup()
        )
        await manager.cleanup_started.wait()
        second = asyncio.create_task(
            ProxyStartupReconciler(factory, manager, owner="executor-b").reconcile_startup()
        )
        await asyncio.sleep(0.15)
        assert not second.done()
        manager.cleanup_release.set()

        first_report, second_report = await asyncio.gather(first, second)

        assert first_report.removed_count == 1
        assert second_report.decisions == ()
        assert manager.cleaned == [manifest]
    finally:
        await engine.dispose()


async def test_durable_state_is_reread_after_waiting_for_lock(postgres_dsn: str) -> None:
    engine, factory = storage(postgres_dsn)
    holder = factory()
    try:
        task, run_id, step = await seed_task_run_step(factory)
        await _set_lease(
            factory,
            step.id,
            status=StepStatus.RUNNING,
            generation=1,
            expires_at=datetime.now(UTC) - timedelta(seconds=10),
        )
        manifest = _manifest(task.id, run_id, step.id, 1)
        manager = RecordingProxyManager([manifest])
        await holder.execute(
            text("SELECT pg_advisory_lock(:key)"), {"key": RECONCILIATION_LOCK_KEY}
        )
        reconciliation = asyncio.create_task(
            ProxyStartupReconciler(factory, manager, owner="executor-b").reconcile_startup()
        )
        await asyncio.sleep(0.15)
        await _set_lease(
            factory,
            step.id,
            status=StepStatus.RUNNING,
            generation=1,
            expires_at=datetime.now(UTC) + timedelta(minutes=2),
        )
        await holder.execute(
            text("SELECT pg_advisory_unlock(:key)"), {"key": RECONCILIATION_LOCK_KEY}
        )
        await holder.commit()

        report = await reconciliation

        assert (
            report.decisions[0].classification
            is ReconciliationClassification.PRESERVED_ACTIVE_LEASE
        )
        assert manager.cleaned == []
    finally:
        await holder.close()
        await engine.dispose()
