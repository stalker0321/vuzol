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
from vuzol.experiments.shadow import load_shadow_records, record_shadow_decision
from vuzol.experiments.snapshot import PolicySnapshot

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
    "PolicySnapshot",
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
    "load_shadow_records",
    "plan_cohort",
    "pricing_comparable",
    "record_shadow_decision",
]
