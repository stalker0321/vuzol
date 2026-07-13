"""Durable fenced startup reconciliation for controlled-egress resources."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.execution.proxy_networks import ProxyNetworkError
from vuzol.execution.proxy_service import (
    ProxyRecoveryManifest,
    ProxyServiceError,
    ProxyServiceManager,
)
from vuzol.storage.models import Event, Run, Step, Task
from vuzol.storage.types import RunStatus, StepStatus

RECONCILIATION_LOCK_KEY = 8_946_527_104


class ReconciliationClassification(StrEnum):
    PRESERVED_ACTIVE_LEASE = "PRESERVED_ACTIVE_LEASE"
    REMOVED_TERMINAL_LEFTOVER = "REMOVED_TERMINAL_LEFTOVER"
    REMOVED_EXPIRED_LEASE = "REMOVED_EXPIRED_LEASE"
    REMOVED_OLD_GENERATION = "REMOVED_OLD_GENERATION"
    PRESERVED_AMBIGUOUS = "PRESERVED_AMBIGUOUS"
    PRESERVED_FOREIGN = "PRESERVED_FOREIGN"
    CLEANUP_FAILED = "CLEANUP_FAILED"
    ALREADY_ABSENT = "ALREADY_ABSENT"


@dataclass(frozen=True, slots=True)
class DurableLeaseState:
    identity_consistent: bool
    run_status: RunStatus
    step_status: StepStatus
    current_generation: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None


@dataclass(frozen=True, slots=True)
class ReconciliationDecision:
    step_id: str
    lease_generation: int
    classification: ReconciliationClassification


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    lock_acquired: bool
    decisions: tuple[ReconciliationDecision, ...]

    @property
    def removed_count(self) -> int:
        removable = {
            ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER,
            ReconciliationClassification.REMOVED_EXPIRED_LEASE,
            ReconciliationClassification.REMOVED_OLD_GENERATION,
        }
        return sum(decision.classification in removable for decision in self.decisions)


def classify_recovery_manifest(
    manifest: ProxyRecoveryManifest,
    state: DurableLeaseState | None,
    *,
    now: datetime,
) -> ReconciliationClassification:
    if state is None or not state.identity_consistent:
        return ReconciliationClassification.PRESERVED_AMBIGUOUS
    if manifest.lease_generation < state.current_generation:
        return ReconciliationClassification.REMOVED_OLD_GENERATION
    if manifest.lease_generation > state.current_generation:
        return ReconciliationClassification.PRESERVED_AMBIGUOUS
    terminal_steps = {
        StepStatus.BLOCKED,
        StepStatus.FAILED,
        StepStatus.CANCELLED,
        StepStatus.COMPLETED,
    }
    terminal_runs = {
        RunStatus.BLOCKED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.COMPLETED,
    }
    if state.step_status in terminal_steps or state.run_status in terminal_runs:
        return ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER
    active_status = state.step_status in {StepStatus.LEASED, StepStatus.RUNNING}
    if (
        active_status
        and state.run_status is RunStatus.RUNNING
        and state.lease_owner is not None
        and state.lease_expires_at is not None
        and state.heartbeat_at is not None
        and state.lease_expires_at >= now
    ):
        return ReconciliationClassification.PRESERVED_ACTIVE_LEASE
    if active_status and state.lease_expires_at is not None and state.lease_expires_at < now:
        return ReconciliationClassification.REMOVED_EXPIRED_LEASE
    return ReconciliationClassification.PRESERVED_AMBIGUOUS


class ProxyStartupReconciler:
    """Serialize startup cleanup and authorize every mutation from row-locked PostgreSQL state."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        manager: ProxyServiceManager,
        *,
        owner: str,
        lock_timeout_seconds: float = 5.0,
        lock_poll_seconds: float = 0.1,
    ) -> None:
        self._factory = factory
        self._manager = manager
        self._owner = owner
        self._lock_timeout_seconds = lock_timeout_seconds
        self._lock_poll_seconds = lock_poll_seconds

    async def reconcile_startup(self) -> ReconciliationReport:
        async with self._factory() as lock_session:
            connection = await lock_session.connection()
            if not await self._acquire_lock(lock_session):
                return ReconciliationReport(lock_acquired=False, decisions=())
            decisions: list[ReconciliationDecision] = []
            try:
                manifests = self._manager.recovery_manifests()
                for manifest in manifests:
                    async with self._factory() as state_session:
                        decisions.append(await self._reconcile_one(state_session, manifest))
                return ReconciliationReport(lock_acquired=True, decisions=tuple(decisions))
            finally:
                try:
                    await lock_session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": RECONCILIATION_LOCK_KEY},
                    )
                    await lock_session.commit()
                except Exception:
                    await connection.invalidate()
                    raise

    async def _acquire_lock(self, session: AsyncSession) -> bool:
        deadline = time.monotonic() + self._lock_timeout_seconds
        while True:
            acquired = await session.scalar(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": RECONCILIATION_LOCK_KEY},
            )
            if acquired is True:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(self._lock_poll_seconds)

    async def _reconcile_one(
        self, session: AsyncSession, manifest: ProxyRecoveryManifest
    ) -> ReconciliationDecision:
        async with session.begin():
            state = await self._read_state_for_update(session, manifest)
            classification = classify_recovery_manifest(
                manifest,
                state,
                now=datetime.now(UTC),
            )
            if classification in {
                ReconciliationClassification.PRESERVED_ACTIVE_LEASE,
                ReconciliationClassification.PRESERVED_AMBIGUOUS,
            }:
                await self._record(session, manifest, classification)
                return self._decision(manifest, classification)
            try:
                await self._manager.validate_recovery_resources(manifest)
            except (ProxyNetworkError, ProxyServiceError) as error:
                preserved = (
                    ReconciliationClassification.PRESERVED_FOREIGN
                    if "foreign" in str(error).lower()
                    else ReconciliationClassification.PRESERVED_AMBIGUOUS
                )
                await self._record(session, manifest, preserved)
                return self._decision(manifest, preserved)
            try:
                await self._manager.cleanup_recovery_manifest(manifest)
            except (ProxyNetworkError, ProxyServiceError):
                await self._record(
                    session,
                    manifest,
                    ReconciliationClassification.CLEANUP_FAILED,
                )
                return self._decision(
                    manifest,
                    ReconciliationClassification.CLEANUP_FAILED,
                )
            await self._record(session, manifest, classification)
            return self._decision(manifest, classification)

    @staticmethod
    async def _read_state_for_update(
        session: AsyncSession, manifest: ProxyRecoveryManifest
    ) -> DurableLeaseState | None:
        step = await session.scalar(
            select(Step).where(Step.id == manifest.step_id).with_for_update()
        )
        run = await session.scalar(select(Run).where(Run.id == manifest.run_id).with_for_update())
        task = await session.scalar(
            select(Task).where(Task.id == manifest.task_id).with_for_update()
        )
        if step is None or run is None or task is None:
            return None
        return DurableLeaseState(
            identity_consistent=(
                step.run_id == run.id
                and run.task_id == task.id
                and step.run_id == manifest.run_id
                and run.task_id == manifest.task_id
            ),
            run_status=run.status,
            step_status=step.status,
            current_generation=step.lease_generation,
            lease_owner=step.lease_owner,
            lease_expires_at=step.lease_expires_at,
            heartbeat_at=step.heartbeat_at,
        )

    async def _record(
        self,
        session: AsyncSession,
        manifest: ProxyRecoveryManifest,
        classification: ReconciliationClassification,
    ) -> None:
        session.add(
            Event(
                entity_type="runtime_resource",
                entity_id=manifest.step_id,
                event_type="execution.startup_reconciliation",
                actor_type="executor",
                actor_id=self._owner,
                payload={
                    "classification": classification.value,
                    "task_id": str(manifest.task_id),
                    "run_id": str(manifest.run_id),
                    "step_id": str(manifest.step_id),
                    "lease_generation": manifest.lease_generation,
                },
            )
        )
        await session.flush()

    @staticmethod
    def _decision(
        manifest: ProxyRecoveryManifest,
        classification: ReconciliationClassification,
    ) -> ReconciliationDecision:
        return ReconciliationDecision(
            step_id=str(manifest.step_id),
            lease_generation=manifest.lease_generation,
            classification=classification,
        )
