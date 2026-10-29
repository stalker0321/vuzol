"""Durable fenced startup reconciliation for controlled-egress resources."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import String, and_, cast, select, text, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.execution.domain import ProcessEnvelope
from vuzol.execution.ports import SandboxRuntime
from vuzol.execution.proxy_networks import ProxyNetworkError
from vuzol.execution.proxy_service import (
    ProxyRecoveryManifest,
    ProxyServiceError,
    ProxyServiceManager,
)
from vuzol.execution.turn_transport import cleanup_turn_runtime, turn_runtime_lock
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import TurnInvocationContext, invocation_current
from vuzol.leadership.persistence import LeadershipInference, LeadershipTurn, ManagedGoal
from vuzol.providers.account_persistence import (
    ProviderAccountLease,
    release_account_ticket_after_cleanup,
)
from vuzol.storage.models import (
    Event,
    Run,
    Step,
    SupervisedProcess,
    Task,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.types import ProcessOutcome, ProcessStatus, RunStatus, StepStatus

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
        self._ticket_cursor: uuid.UUID | None = None

    async def reconcile_startup(self) -> ReconciliationReport:
        async with self._factory() as lock_session:
            connection = await lock_session.connection()
            if not await self._acquire_lock(lock_session):
                return ReconciliationReport(lock_acquired=False, decisions=())
            decisions: list[ReconciliationDecision] = []
            try:
                manifests = self._manager.recovery_manifests()
                for manifest in manifests:
                    # These belong to TurnStartupReconciler, never Step-based
                    # authority or event identities.
                    if manifest.turn_binding is not None:
                        continue
                    async with self._factory() as state_session:
                        decisions.append(await self._reconcile_one(state_session, manifest))
                self._ticket_cursor = await release_reaped_account_tickets(
                    self._factory, self._ticket_cursor
                )
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
            processes = await session.scalars(
                select(SupervisedProcess)
                .join(Worktree, Worktree.id == SupervisedProcess.worktree_id)
                .where(
                    SupervisedProcess.task_id == manifest.task_id,
                    SupervisedProcess.run_id == manifest.run_id,
                    SupervisedProcess.step_id == manifest.step_id,
                    SupervisedProcess.lease_generation == manifest.lease_generation,
                    SupervisedProcess.leadership_turn_id.is_(None),
                    SupervisedProcess.invocation_id.is_(None),
                    Worktree.task_id == manifest.task_id,
                    Worktree.run_id == manifest.run_id,
                )
                .with_for_update(of=SupervisedProcess)
            )
            for process in processes:
                mark_reaped(process)
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


def mark_reaped(process: SupervisedProcess) -> None:
    """Only call after exact ownership validation AND confirmed runtime cleanup."""
    process.status = ProcessStatus.EXITED
    process.outcome = process.outcome or ProcessOutcome.UNKNOWN
    process.ended_at = process.ended_at or datetime.now(UTC)
    process.reaped_at = datetime.now(UTC)
    process.runtime_metadata = {
        **process.runtime_metadata,
        "cleanup_confirmed": True,
        "startup_cleanup_confirmed": True,
    }


async def release_reaped_account_tickets(
    factory: async_sessionmaker[AsyncSession], after_id: uuid.UUID | None = None
) -> uuid.UUID | None:
    """Also retry the crash gap between committed cleanup proof and ticket CAS."""
    async with factory() as session:
        query = (
            select(
                SupervisedProcess.id,
                SupervisedProcess.profile_id,
                SupervisedProcess.runtime_metadata,
            )
            .join(
                ProviderAccountLease,
                and_(
                    ProviderAccountLease.account_id
                    == SupervisedProcess.runtime_metadata["account_id"].as_string(),
                    cast(ProviderAccountLease.token, String)
                    == SupervisedProcess.runtime_metadata["account_lease_token"].as_string(),
                ),
            )
            .where(
                SupervisedProcess.status == ProcessStatus.EXITED,
                SupervisedProcess.reaped_at.is_not(None),
                SupervisedProcess.runtime_metadata["cleanup_confirmed"].as_boolean().is_(True),
                SupervisedProcess.runtime_metadata["launcher_drained"].as_boolean().is_(True),
                SupervisedProcess.runtime_metadata["account_lease_token"].as_string().is_not(None),
            )
            .order_by(SupervisedProcess.id)
            .limit(100)
        )
        if after_id is not None:
            query = query.where(SupervisedProcess.id > after_id)
        records = (await session.execute(query)).all()
    # No process/goal/step row locks may be held while acquiring the account lock.
    for _, profile_id, metadata in records:
        account_id, slot = metadata.get("account_id"), metadata.get("account_slot")
        try:
            token = uuid.UUID(metadata["account_lease_token"])
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
        if not isinstance(account_id, str) or type(slot) is not int or not 0 <= slot < 100:
            continue
        await release_account_ticket_after_cleanup(
            factory,
            account_id=account_id,
            slot=slot,
            token=token,
            account_profile_ids=(profile_id,),
            cleanup_confirmed=True,
        )
    return records[-1][0] if len(records) == 100 else None


class TurnStartupReconciler:
    """Reap stale bound resources, without inferring launcher death from absence.

    `enabled` must read the current runtime setting, not a serialized true flag.
    The lock preserves cooperating live launchers. A lost DB connection is not
    death proof: tickets without a normal-path launcher_drained attestation stay
    reserved even after startup observes/removes their exact resources.
    """

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        runtime: SandboxRuntime,
        proxy_manager: ProxyServiceManager | None = None,
        *,
        owner: str,
        enabled: Callable[[], bool],
    ) -> None:
        self._factory = factory
        self._runtime = runtime
        self._proxy = proxy_manager
        self._owner = owner
        self._enabled = enabled
        self._cursor: tuple[datetime, uuid.UUID] | None = None
        self._ticket_cursor: uuid.UUID | None = None

    async def reconcile_startup(self) -> ReconciliationReport:
        async with self._factory() as session:
            query = (
                select(
                    SupervisedProcess.id,
                    SupervisedProcess.invocation_id,
                    SupervisedProcess.lease_generation,
                    SupervisedProcess.created_at,
                )
                .where(
                    SupervisedProcess.leadership_turn_id.is_not(None),
                    SupervisedProcess.reaped_at.is_(None),
                )
                .order_by(SupervisedProcess.created_at, SupervisedProcess.id)
                .limit(100)
            )
            if self._cursor is not None:
                query = query.where(
                    tuple_(SupervisedProcess.created_at, SupervisedProcess.id) > self._cursor
                )
            identities = (await session.execute(query)).all()
        self._cursor = (identities[-1][3], identities[-1][0]) if len(identities) == 100 else None
        decisions = []
        for process_id, invocation_id, generation, _ in identities:
            async with turn_runtime_lock(self._factory, invocation_id) as connection:
                if connection is None:
                    classification = ReconciliationClassification.PRESERVED_ACTIVE_LEASE
                else:
                    classification = await self._reconcile_one(process_id)
            decisions.append(
                ReconciliationDecision(
                    # The historical report calls this field step_id; it is a
                    # diagnostic identifier only, never a workflow foreign key.
                    step_id=str(invocation_id),
                    lease_generation=generation,
                    classification=classification,
                )
            )
        self._ticket_cursor = await release_reaped_account_tickets(
            self._factory, self._ticket_cursor
        )
        return ReconciliationReport(lock_acquired=True, decisions=tuple(decisions))

    async def _reconcile_one(self, process_id: uuid.UUID) -> ReconciliationClassification:
        async with self._factory.begin() as session:
            process = await session.get(SupervisedProcess, process_id)
            if process is None:
                return ReconciliationClassification.PRESERVED_AMBIGUOUS
            if process.reaped_at is not None:
                return ReconciliationClassification.ALREADY_ABSENT
            try:
                context, envelope = recovered_turn_binding(process, enabled=self._enabled)
            except (ValueError, KeyError, TypeError, AttributeError):
                return await self._record(
                    session, process, ReconciliationClassification.PRESERVED_AMBIGUOUS
                )
            # Same lock order as the live authority checker (goal, turn,
            # outbox, then process); missing evidence is not revocation proof.
            goal = await session.get(ManagedGoal, context.claim.goal_id, with_for_update=True)
            turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=True)
            if context.job:
                from vuzol.leadership.persistence import LeadJob

                job = await session.get(LeadJob, context.claim.wakeup_id, with_for_update=True)
                authority_missing = (
                    job is None
                    or job.goal_id != context.claim.goal_id
                    or job.lease_generation < context.claim.lease_epoch
                    or turn is None
                    or turn.wakeup_id != turn.id
                    or turn.id
                    != uuid.uuid5(context.claim.wakeup_id, f"attempt:{context.claim.lease_epoch}")
                )
                current_generation = job.lease_generation if job is not None else 0
            else:
                wakeup = await session.get(
                    TransactionalOutbox, context.claim.wakeup_id, with_for_update=True
                )
                authority_missing = (
                    wakeup is None
                    or turn is None
                    or turn.wakeup_id != context.claim.wakeup_id
                    or goal is None
                    or goal.lease_epoch < context.claim.lease_epoch
                    or wakeup.lease_generation < context.claim.outbox_generation
                )
                current_generation = goal.lease_epoch if goal is not None else 0
            phase = await session.get(LeadershipInference, process.invocation_id)
            if (
                goal is None
                or turn is None
                or phase is None
                or authority_missing
                or goal.project_id != context.project_id
                or turn.goal_id != goal.id
                or phase.turn_id != turn.id
                or phase.profile_id != process.profile_id
            ):
                return await self._record(
                    session, process, ReconciliationClassification.PRESERVED_AMBIGUOUS
                )
            if await invocation_current(session, context):
                return await self._record(
                    session, process, ReconciliationClassification.PRESERVED_ACTIVE_LEASE
                )
            await session.refresh(process, with_for_update=True)
            try:
                await cleanup_turn_runtime(self._runtime, self._proxy, envelope)
            except Exception:
                process.status = ProcessStatus.UNKNOWN
                process.outcome = process.outcome or ProcessOutcome.UNKNOWN
                process.runtime_metadata = {**process.runtime_metadata, "cleanup_confirmed": False}
                return await self._record(
                    session, process, ReconciliationClassification.CLEANUP_FAILED
                )
            mark_reaped(process)
            classification = (
                ReconciliationClassification.REMOVED_OLD_GENERATION
                if current_generation > context.claim.lease_epoch
                else ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER
                if goal.lifecycle != "active" or turn.status != "claimed" or not self._enabled()
                else ReconciliationClassification.REMOVED_EXPIRED_LEASE
            )
            return await self._record(session, process, classification)

    async def _record(
        self,
        session: AsyncSession,
        process: SupervisedProcess,
        classification: ReconciliationClassification,
    ) -> ReconciliationClassification:
        session.add(
            Event(
                entity_type="runtime_resource",
                entity_id=process.invocation_id,
                event_type="execution.startup_reconciliation",
                actor_type="executor",
                actor_id=self._owner,
                payload={
                    "classification": classification.value,
                    "binding": "leadership-turn",
                    "turn_id": str(process.leadership_turn_id),
                    "invocation_id": str(process.invocation_id),
                    "lease_generation": process.lease_generation,
                },
            )
        )
        await session.flush()
        return classification


def recovered_turn_binding(
    process: SupervisedProcess, *, enabled: Callable[[], bool]
) -> tuple[TurnInvocationContext, ProcessEnvelope]:
    from vuzol.execution.turn_transport import valid_turn_workspace

    payload = process.runtime_metadata["turn_context"]
    for key in ("lease_epoch", "outbox_generation", "version", "fence_epoch", "scope_revision"):
        if type(payload[key]) is not int or payload[key] < 1:
            raise ValueError("malformed recovered turn fencing identity")
    if not isinstance(payload["owner"], str) or not payload["owner"]:
        raise ValueError("missing turn launch owner")
    if type(payload.get("conversational", False)) is not bool:
        raise ValueError("malformed recovered turn conversational binding")
    if type(payload.get("retrospective", False)) is not bool:
        raise ValueError("malformed recovered turn retrospective binding")
    if type(payload.get("job", False)) is not bool:
        raise ValueError("malformed recovered job binding")
    context = TurnInvocationContext(
        claim=TurnClaim(
            goal_id=uuid.UUID(payload["goal_id"]),
            wakeup_id=uuid.UUID(payload["wakeup_id"]),
            owner=payload["owner"],
            lease_epoch=payload["lease_epoch"],
            outbox_generation=payload["outbox_generation"],
        ),
        turn_id=uuid.UUID(payload["turn_id"]),
        project_id=payload["project_id"],
        version=payload["version"],
        fence_epoch=payload["fence_epoch"],
        scope_revision=payload["scope_revision"],
        conversational=payload.get("conversational", False),
        retrospective=payload.get("retrospective", False),
        job=payload.get("job", False),
        enabled=enabled,
    )
    envelope = ProcessEnvelope.model_validate(
        {**process.runtime_metadata["runtime_envelope"], "stdin": ""}
    )
    binding = envelope.turn_binding
    if (
        binding is None
        or binding.turn_id != process.leadership_turn_id
        or binding.invocation_id != process.invocation_id
        or context.turn_id != binding.turn_id
        or context.claim.lease_epoch != process.lease_generation
        or envelope.lease_generation != process.lease_generation
        or envelope.profile_id != process.profile_id
        or envelope.provider_attempt != process.provider_attempt
        or envelope.container_name != process.container_id
        or envelope.sandbox.stable_hash != process.sandbox_spec_hash
        or list(envelope.argv) != process.command_envelope.get("argv")
        or not valid_turn_workspace(envelope)
        or any(
            value is not None
            for value in (process.task_id, process.run_id, process.step_id, process.worktree_id)
        )
    ):
        raise ValueError("recovered turn binding does not match durable process ownership")
    return context, envelope
