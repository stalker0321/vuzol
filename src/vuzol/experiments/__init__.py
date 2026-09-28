"""Bounded adaptive-worker experiment contracts and policy."""

from vuzol.experiments.analysis import HYPOTHESES, TrialRecord
from vuzol.experiments.arms import ExperimentArm, PlannedRun, plan_cohort
from vuzol.experiments.corpus import CorpusManifest, CorpusSplit, CorpusStratum, CorpusTask
from vuzol.experiments.domain import (
    ContextManifest,
    ExecutionStrategy,
    ReviewOutcome,
    TaskClassification,
    WorkerEditReport,
    WorkerResultManifest,
    WorkerTaskCapsule,
)
from vuzol.experiments.export import joint_export, pricing_comparable
from vuzol.experiments.policy import classify_execution_strategy
from vuzol.experiments.snapshot import PolicySnapshot

__all__ = [
    "HYPOTHESES",
    "ContextManifest",
    "CorpusManifest",
    "CorpusSplit",
    "CorpusStratum",
    "CorpusTask",
    "ExecutionStrategy",
    "ExperimentArm",
    "PlannedRun",
    "PolicySnapshot",
    "ReviewOutcome",
    "TaskClassification",
    "TrialRecord",
    "WorkerEditReport",
    "WorkerResultManifest",
    "WorkerTaskCapsule",
    "classify_execution_strategy",
    "joint_export",
    "plan_cohort",
    "pricing_comparable",
]
