"""Paired arm execution paths for the controlled harness (WP13, D6 Q4).

Baseline modes plus the D6 candidate delta (T001 baseline §2 extended):
a label alone is not a topology (EXPERIMENTS.md §7):

- current: fixed compiled workflow with a human approval step (arm A);
- strong_solo: one bounded owner loop, no separate approval step (arm B,
  efficient spend by definition — the documented divergence);
- hybrid: deterministic procedure with risk-based review before approval;
- candidate_delta: full D-stack trial (arm C) — production capabilities,
  risk-based review, human approval (same approval topology as A, so A/C
  never compare approval against no-approval), plus an acceptance-evidence
  recording step (D2). Balanced spend keeps the budget-mode set closed;
  the economic comparison gates on measured cost, not mode labels.

Paired runs share ``pair_id`` (corpus task + seed) across arms; execution
order is randomized with an explicit recorded seed. Live model benchmark is
forbidden — CI replays deterministic fixture outcomes through
``run_fixture_cohort``; real execution stays on the ``seed_trial`` path.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TypedDict

from vuzol.config.models import Capability
from vuzol.experiments.corpus import CorpusManifest, CorpusTask
from vuzol.experiments.service import _trial_workflow
from vuzol.storage.types import (
    IdempotencyClass,
    QueueClass,
    RetryClass,
    StepStatus,
)
from vuzol.workflows.domain import MaterializedStep, MaterializedWorkflow


class ExperimentArm(StrEnum):
    CURRENT = "current"
    STRONG_SOLO = "strong_solo"
    HYBRID = "hybrid"
    CANDIDATE_DELTA = "candidate_delta"


_ARM_WORKFLOW_TYPES = {
    ExperimentArm.CURRENT: "adaptive_worker_trial",
    ExperimentArm.STRONG_SOLO: "adaptive_worker_trial_solo",
    ExperimentArm.HYBRID: "adaptive_worker_trial_hybrid",
    ExperimentArm.CANDIDATE_DELTA: "adaptive_worker_trial_delta",
}


_ARM_BUDGET_MODES = {
    ExperimentArm.CURRENT: "strong",
    ExperimentArm.STRONG_SOLO: "efficient",
    ExperimentArm.HYBRID: "balanced",
    ExperimentArm.CANDIDATE_DELTA: "balanced",
}


@dataclass(frozen=True, slots=True)
class PlannedRun:
    pair_id: str
    corpus_task_id: str
    family: str
    arm: ExperimentArm
    seed: int
    order_index: int


def pair_id_for(corpus_task_id: str, seed: int) -> str:
    return f"{corpus_task_id}:seed-{seed}"


def materialize_arm_workflow(
    arm: ExperimentArm,
    interpretation_id: uuid.UUID,
    timeout: int,
    *,
    runtime_certification: bool = False,
) -> MaterializedWorkflow:
    """Build the documented execution path of one arm (no model calls)."""

    if arm is ExperimentArm.CURRENT:
        return _trial_workflow(
            interpretation_id, timeout, runtime_certification=runtime_certification
        )
    if arm is ExperimentArm.STRONG_SOLO:
        return MaterializedWorkflow(
            workflow_type=_ARM_WORKFLOW_TYPES[arm],
            version="1",
            interpretation_id=interpretation_id,
            steps=(
                _interpret_step(),
                _prepare_step(timeout),
                MaterializedStep(
                    ordinal=2,
                    key="execute_code",
                    step_type="execute_code",
                    predecessor_ordinals=(1,),
                    queue_class=QueueClass.HEAVY,
                    capabilities=frozenset(),
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                    timeout_seconds=timeout,
                    max_attempts=1,
                    priority=100,
                    payload={"solo_owner": True, "budget_mode": _ARM_BUDGET_MODES[arm]},
                ),
            ),
        )
    if arm is ExperimentArm.CANDIDATE_DELTA:
        return MaterializedWorkflow(
            workflow_type=_ARM_WORKFLOW_TYPES[arm],
            version="1",
            interpretation_id=interpretation_id,
            steps=(
                _interpret_step(),
                _prepare_step(timeout),
                MaterializedStep(
                    ordinal=2,
                    key="execute_code",
                    step_type="execute_code",
                    predecessor_ordinals=(1,),
                    queue_class=QueueClass.HEAVY,
                    capabilities=frozenset({Capability.CODE_EDIT, Capability.PROJECT_SHELL}),
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                    timeout_seconds=timeout,
                    max_attempts=1,
                    priority=100,
                    payload={
                        "budget_mode": _ARM_BUDGET_MODES[arm],
                        "d_stack": "full",
                    },
                ),
                MaterializedStep(
                    ordinal=3,
                    key="review_result",
                    step_type="review",
                    predecessor_ordinals=(2,),
                    queue_class=QueueClass.HEAVY,
                    capabilities=frozenset(),
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    timeout_seconds=600,
                    max_attempts=1,
                    priority=100,
                    payload={"review_mode": "risk_based_boundary"},
                ),
                MaterializedStep(
                    ordinal=4,
                    key="approve_result",
                    step_type="approval",
                    predecessor_ordinals=(3,),
                    queue_class=QueueClass.PRIVILEGED,
                    capabilities=frozenset({Capability.GIT}),
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.IDEMPOTENT,
                    timeout_seconds=120,
                    max_attempts=2,
                    priority=100,
                    payload={"requested_action": "apply_result"},
                ),
                MaterializedStep(
                    ordinal=5,
                    key="record_evidence",
                    step_type="record_evidence",
                    predecessor_ordinals=(4,),
                    queue_class=QueueClass.LIGHT,
                    capabilities=frozenset(),
                    retry_class=RetryClass.NEVER,
                    idempotency_class=IdempotencyClass.READ_ONLY,
                    timeout_seconds=120,
                    max_attempts=1,
                    priority=100,
                    payload={"d_stack": "full", "evidence_kind": "acceptance"},
                ),
            ),
        )
    return MaterializedWorkflow(
        workflow_type=_ARM_WORKFLOW_TYPES[arm],
        version="1",
        interpretation_id=interpretation_id,
        steps=(
            _interpret_step(),
            _prepare_step(timeout),
            MaterializedStep(
                ordinal=2,
                key="execute_code",
                step_type="execute_code",
                predecessor_ordinals=(1,),
                queue_class=QueueClass.HEAVY,
                capabilities=frozenset(),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                timeout_seconds=timeout,
                max_attempts=1,
                priority=100,
                payload={"budget_mode": _ARM_BUDGET_MODES[arm]},
            ),
            MaterializedStep(
                ordinal=3,
                key="review_result",
                step_type="review",
                predecessor_ordinals=(2,),
                queue_class=QueueClass.HEAVY,
                capabilities=frozenset(),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.READ_ONLY,
                timeout_seconds=600,
                max_attempts=1,
                priority=100,
                payload={"review_mode": "risk_based_boundary"},
            ),
            MaterializedStep(
                ordinal=4,
                key="approve_result",
                step_type="approval",
                predecessor_ordinals=(3,),
                queue_class=QueueClass.PRIVILEGED,
                capabilities=frozenset(),
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                timeout_seconds=120,
                max_attempts=2,
                priority=100,
                payload={"requested_action": "apply_result"},
            ),
        ),
    )


def _interpret_step() -> MaterializedStep:
    return MaterializedStep(
        ordinal=0,
        key="interpret",
        step_type="interpret",
        predecessor_ordinals=(),
        queue_class=QueueClass.LIGHT,
        capabilities=frozenset(),
        retry_class=RetryClass.NEVER,
        idempotency_class=IdempotencyClass.READ_ONLY,
        timeout_seconds=60,
        max_attempts=1,
        priority=100,
        status=StepStatus.COMPLETED,
    )


def _prepare_step(timeout: int) -> MaterializedStep:
    del timeout
    return MaterializedStep(
        ordinal=1,
        key="prepare_worktree",
        step_type="prepare_worktree",
        predecessor_ordinals=(0,),
        queue_class=QueueClass.HEAVY,
        capabilities=frozenset(),
        retry_class=RetryClass.NEVER,
        idempotency_class=IdempotencyClass.ISOLATED_RETRYABLE,
        timeout_seconds=600,
        max_attempts=1,
        priority=100,
    )


class PathStep(TypedDict):
    ordinal: int
    key: str
    step_type: str


class ExecutionPath(TypedDict):
    arm: str
    workflow_type: str
    budget_mode: str
    steps: list[PathStep]


def describe_execution_path(arm: ExperimentArm) -> ExecutionPath:
    """Documented execution path of one arm (steps, roles, budget mode)."""

    interpretation_id = uuid.uuid4()
    workflow = materialize_arm_workflow(arm, interpretation_id, timeout=1_800)
    return {
        "arm": arm.value,
        "workflow_type": workflow.workflow_type,
        "budget_mode": _ARM_BUDGET_MODES[arm],
        "steps": [
            {"ordinal": step.ordinal, "key": step.key, "step_type": step.step_type}
            for step in workflow.steps
        ],
    }


def plan_cohort(
    corpus: CorpusManifest,
    arms: tuple[ExperimentArm, ...],
    seeds: tuple[int, ...],
    *,
    shuffle_seed: int,
    only_smoke: bool = False,
) -> tuple[PlannedRun, ...]:
    """Pair every corpus task x seed across arms; randomize order deterministically."""

    tasks: tuple[CorpusTask, ...] = corpus.smoke_tasks() if only_smoke else corpus.tasks
    planned: list[PlannedRun] = [
        PlannedRun(
            pair_id=pair_id_for(task.task_id, seed),
            corpus_task_id=task.task_id,
            family=task.family,
            arm=arm,
            seed=seed,
            order_index=0,
        )
        for task in tasks
        for seed in seeds
        for arm in arms
    ]
    rng = random.Random(shuffle_seed)  # noqa: S311 - run order shuffle, not security
    rng.shuffle(planned)
    return tuple(
        PlannedRun(
            pair_id=run.pair_id,
            corpus_task_id=run.corpus_task_id,
            family=run.family,
            arm=run.arm,
            seed=run.seed,
            order_index=index,
        )
        for index, run in enumerate(planned)
    )
