"""Bounded adaptive-worker experiment contracts and policy."""

from vuzol.experiments.analysis import HYPOTHESES, TrialRecord
from vuzol.experiments.arms import ExperimentArm, PlannedRun, plan_cohort
from vuzol.experiments.corpus import CorpusManifest, CorpusSplit, CorpusStratum, CorpusTask
from vuzol.experiments.decision import (
    DECISION_SCHEMA,
    DecisionInvalid,
    DecisionNotExecutable,
    DecisionStale,
    ReasonCode,
    TriageChoice,
    TriageDecision,
    WhitelistGate,
    abstain_decision,
    authorize_execution,
    interpret_output,
)
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

__all__ = [
    "DECISION_SCHEMA",
    "HYPOTHESES",
    "ContextManifest",
    "CorpusManifest",
    "CorpusSplit",
    "CorpusStratum",
    "CorpusTask",
    "DecisionInvalid",
    "DecisionNotExecutable",
    "DecisionStale",
    "ExecutionStrategy",
    "ExperimentArm",
    "PlannedRun",
    "ReasonCode",
    "ReviewOutcome",
    "TaskClassification",
    "TriageChoice",
    "TriageDecision",
    "TrialRecord",
    "WhitelistGate",
    "WorkerEditReport",
    "WorkerResultManifest",
    "WorkerTaskCapsule",
    "abstain_decision",
    "authorize_execution",
    "classify_execution_strategy",
    "interpret_output",
    "joint_export",
    "plan_cohort",
    "pricing_comparable",
]
