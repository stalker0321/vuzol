"""Durable effect reconciliation for the Git CAS apply path (WP05).

Sibling of ``ProxyStartupReconciler`` (same advisory-lock + row-locked state +
classification pattern) without touching the proxy/egress semantics. It observes
the real target (the Git ref) by ``operation_key`` context and settles the
effect to applied/denied/uncertain. It never moves the ref and never launches an
effect.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.registries import ConfigurationBundle
from vuzol.execution.effect import (
    METHOD_READ_GIT_REF,
    STATUS_DISPATCHED,
    STATUS_INTENT_RECORDED,
    STATUS_UNCERTAIN,
    TARGET_KIND_GIT_REF,
    active_effects,
    mark_denied,
    mark_uncertain,
    settle_applied,
)
from vuzol.execution.git import GitError, LocalGit
from vuzol.storage.models import (
    Approval,
    Effect,
    Event,
    MaterializationLink,
    Step,
    WorkPackage,
    Worktree,
)
from vuzol.storage.types import ApprovalStatus, StepStatus, WorktreeDeliveryState

EFFECT_RECONCILIATION_LOCK_KEY = 8_946_527_105


class EffectObservation(StrEnum):
    APPLIED = "applied"
    NOT_APPLIED = "not_applied"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True, slots=True)
class EffectReconciliationDecision:
    effect_id: str
    operation_key: str
    classification: EffectObservation


@dataclass(frozen=True, slots=True)
class EffectReconciliationReport:
    lock_acquired: bool
    decisions: tuple[EffectReconciliationDecision, ...]

    @property
    def confirmed_count(self) -> int:
        return sum(
            decision.classification is EffectObservation.APPLIED for decision in self.decisions
        )

    @property
    def denied_count(self) -> int:
        return sum(
            decision.classification is EffectObservation.NOT_APPLIED for decision in self.decisions
        )

    @property
    def uncertain_count(self) -> int:
        return sum(
            decision.classification is EffectObservation.UNCERTAIN for decision in self.decisions
        )


def classify_effect_observation(
    *, observed_ref: str | None, result_commit: str, expected_head: str
) -> EffectObservation:
    """Pure observation taxonomy. Unproven state fails closed to uncertain."""

    if observed_ref is None:
        return EffectObservation.UNCERTAIN
    if observed_ref == result_commit:
        return EffectObservation.APPLIED
    if observed_ref == expected_head:
        return EffectObservation.NOT_APPLIED
    return EffectObservation.UNCERTAIN


class EffectReconciler:
    """Observe the target of an unsettled effect and settle it without re-running it."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        git: LocalGit,
        registries: ConfigurationBundle,
        *,
        owner: str,
        lock_timeout_seconds: float = 5.0,
        lock_poll_seconds: float = 0.1,
        batch_size: int = 100,
    ) -> None:
        self._factory = factory
        self._git = git
        self._registries = registries
        self._owner = owner
        self._lock_timeout_seconds = lock_timeout_seconds
        self._lock_poll_seconds = lock_poll_seconds
        self._batch_size = batch_size

    async def reconcile_startup(self) -> EffectReconciliationReport:
        async with self._factory() as lock_session:
            connection = await lock_session.connection()
            if not await self._acquire_lock(lock_session):
                return EffectReconciliationReport(lock_acquired=False, decisions=())
            try:
                async with self._factory() as scan_session:
                    effect_ids = tuple(
                        effect.id
                        for effect in await active_effects(
                            scan_session, limit=self._batch_size
                        )
                    )
                decisions: list[EffectReconciliationDecision] = []
                for effect_id in effect_ids:
                    async with self._factory.begin() as session:
                        decision = await self._reconcile_one(session, effect_id)
                    if decision is not None:
                        decisions.append(decision)
                return EffectReconciliationReport(
                    lock_acquired=True, decisions=tuple(decisions)
                )
            finally:
                try:
                    await lock_session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": EFFECT_RECONCILIATION_LOCK_KEY},
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
                {"key": EFFECT_RECONCILIATION_LOCK_KEY},
            )
            if acquired is True:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(self._lock_poll_seconds)

    async def _reconcile_one(
        self, session: AsyncSession, effect_id: uuid.UUID
    ) -> EffectReconciliationDecision | None:
        effect = await session.scalar(
            select(Effect).where(Effect.id == effect_id).with_for_update()
        )
        if effect is None or effect.status not in (
            STATUS_INTENT_RECORDED,
            STATUS_DISPATCHED,
            STATUS_UNCERTAIN,
        ):
            return None
        observation, observed_ref = await self._observe(effect)
        if observation is EffectObservation.APPLIED:
            settled = await self._settle_business_state(session, effect, observed_ref)
            if not settled:
                mark_uncertain(effect)
                observation = EffectObservation.UNCERTAIN
            else:
                settle_applied(
                    effect,
                    external_ref=f"{effect.target_reference}@{observed_ref}",
                    method=METHOD_READ_GIT_REF,
                )
        elif observation is EffectObservation.NOT_APPLIED:
            mark_denied(effect)
        else:
            mark_uncertain(effect)
            step = await session.get(Step, effect.step_id, with_for_update=True)
            if step is not None and step.status not in {
                StepStatus.BLOCKED,
                StepStatus.FAILED,
                StepStatus.CANCELLED,
                StepStatus.COMPLETED,
            }:
                step.unknown_effects = True
        self._record_event(session, effect, observation)
        await session.flush()
        return EffectReconciliationDecision(
            effect_id=str(effect.id),
            operation_key=effect.operation_key,
            classification=observation,
        )

    async def _observe(self, effect: Effect) -> tuple[EffectObservation, str]:
        context = effect.context if isinstance(effect.context, dict) else {}
        if effect.target_kind != TARGET_KIND_GIT_REF:
            return EffectObservation.UNCERTAIN, ""
        branch = context.get("target_branch")
        result_commit = context.get("result_commit")
        expected_head = context.get("expected_head")
        if not (
            isinstance(branch, str)
            and isinstance(result_commit, str)
            and isinstance(expected_head, str)
        ):
            return EffectObservation.UNCERTAIN, ""
        repository = self._resolve_repository(context)
        if repository is None:
            return EffectObservation.UNCERTAIN, ""
        try:
            observed = await self._git.read_ref(repository, branch)
        except GitError:
            return EffectObservation.UNCERTAIN, ""
        classification = classify_effect_observation(
            observed_ref=observed, result_commit=result_commit, expected_head=expected_head
        )
        return classification, observed or ""

    def _resolve_repository(self, context: dict[str, object]) -> Path | None:
        project_id = context.get("project_id")
        if isinstance(project_id, str):
            try:
                return self._registries.projects.get(project_id).repository_path
            except Exception:
                return None
        raw_path = context.get("repository_path")
        return Path(raw_path) if isinstance(raw_path, str) else None

    async def _settle_business_state(
        self, session: AsyncSession, effect: Effect, observed_ref: str
    ) -> bool:
        """Apply the same business state as a successful apply, without moving Git."""

        context = effect.context if isinstance(effect.context, dict) else {}
        raw_worktree = context.get("worktree_id")
        if not isinstance(raw_worktree, str):
            return False
        try:
            worktree_id = uuid.UUID(raw_worktree)
        except ValueError:
            return False
        worktree = await session.get(Worktree, worktree_id, with_for_update=True)
        if worktree is None or worktree.result_commit != observed_ref:
            return False
        if worktree.delivery_state not in {
            WorktreeDeliveryState.WORKTREE_RETAINED,
            WorktreeDeliveryState.APPLIED,
        }:
            return False
        worktree.delivery_state = WorktreeDeliveryState.APPLIED
        worktree.delivery_operation_hash = effect.payload_hash
        worktree.delivered_ref = effect.target_reference
        if effect.approval_id is not None:
            approval = await session.get(Approval, effect.approval_id, with_for_update=True)
            if approval is not None and approval.status in {
                ApprovalStatus.APPROVED,
                ApprovalStatus.CONSUMED,
            }:
                approval.status = ApprovalStatus.CONSUMED
                approval.consumed_at = approval.consumed_at or datetime.now(UTC)
        if worktree.task_id is not None:
            materialization = await session.scalar(
                select(MaterializationLink).where(MaterializationLink.task_id == worktree.task_id)
            )
            if materialization is not None:
                package = await session.get(
                    WorkPackage, materialization.work_package_id, with_for_update=True
                )
                if (
                    package is not None
                    and effect.target_reference == f"refs/heads/{package.integration_branch}"
                ):
                    package.integration_head_commit = observed_ref
        return True

    def _record_event(
        self, session: AsyncSession, effect: Effect, observation: EffectObservation
    ) -> None:
        session.add(
            Event(
                entity_type="effect",
                entity_id=effect.id,
                event_type="execution.effect_reconciliation",
                actor_type="applier",
                actor_id=self._owner,
                payload={
                    "operation_key": effect.operation_key,
                    "classification": observation.value,
                    "step_id": str(effect.step_id),
                },
            )
        )
