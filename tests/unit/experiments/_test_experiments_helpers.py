import csv
import json
import subprocess
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.cli.experiment import (
    _inspect,
    _parse_args,
    _serialize_artifact,
    _serialize_process,
    _write_csv,
)
from vuzol.experiments.domain import (
    BoundedLevel,
    BoundedRepairContext,
    ContextEntry,
    ContextManifest,
    DefectCategory,
    ExecutionMode,
    ExperimentTelemetry,
    GateResult,
    InvocationTelemetry,
    PricingRevision,
    RepairGateDiagnostic,
    RepairSeverity,
    RepairSymbolContext,
    ReportedUsage,
    RequiredGate,
    ReviewOutcome,
    RiskLevel,
    TaskClass,
    TaskClassification,
    WorkerResultManifest,
    WorkerTaskCapsule,
    new_experiment_id,
    stable_json_hash,
)
from vuzol.experiments.policy import (
    classify_execution_mode,
    enforce_security_escalation,
    scopes_conflict,
)
from vuzol.experiments.review import (
    GitWorkerResultVerifier,
    VerificationResult,
    path_is_allowed,
    scan_suspicious_patterns,
    shadow_auto_accept,
)
from vuzol.experiments.service import TrialSeedRequest, render_worker_prompt
from vuzol.experiments.telemetry import aggregate_trials
from vuzol.providers.routing import _trusted_profile_id
from vuzol.storage.models import Run
from vuzol.storage.types import (
    ArtifactStorageState,
    ProcessOutcome,
    ProcessStatus,
    RunStatus,
    StepStatus,
    WorktreeDeliveryState,
)

__all__ = [
    "UTC",
    "ArtifactStorageState",
    "AsyncMock",
    "AsyncSession",
    "BoundedLevel",
    "BoundedRepairContext",
    "ContextEntry",
    "ContextManifest",
    "Decimal",
    "DefectCategory",
    "ExecutionMode",
    "ExperimentTelemetry",
    "GateResult",
    "GitWorkerResultVerifier",
    "InvocationTelemetry",
    "MagicMock",
    "Path",
    "PricingRevision",
    "ProcessOutcome",
    "ProcessStatus",
    "RepairGateDiagnostic",
    "RepairSeverity",
    "RepairSymbolContext",
    "ReportedUsage",
    "RequiredGate",
    "ReviewOutcome",
    "RiskLevel",
    "Run",
    "RunStatus",
    "StepStatus",
    "TaskClass",
    "TaskClassification",
    "TrialSeedRequest",
    "ValidationError",
    "VerificationResult",
    "WorkerResultManifest",
    "WorkerTaskCapsule",
    "WorktreeDeliveryState",
    "_inspect",
    "_mock_row",
    "_parse_args",
    "_scalar_result",
    "_serialize_artifact",
    "_serialize_process",
    "_trusted_profile_id",
    "_write_csv",
    "aggregate_trials",
    "async_sessionmaker",
    "capsule",
    "classification",
    "classify_execution_mode",
    "csv",
    "datetime",
    "enforce_security_escalation",
    "git",
    "json",
    "new_experiment_id",
    "path_is_allowed",
    "pytest",
    "render_worker_prompt",
    "scan_suspicious_patterns",
    "scopes_conflict",
    "seed_request",
    "shadow_auto_accept",
    "stable_json_hash",
    "subprocess",
    "telemetry",
    "usage",
    "uuid",
    "worker_context",
]


def classification(**updates: object) -> TaskClassification:
    values: dict[str, object] = {
        "task_class": TaskClass.PURE_MODEL_VALIDATOR,
        "complexity": BoundedLevel.LOW,
        "risk": RiskLevel.LOW,
        "testability": BoundedLevel.HIGH,
        "blast_radius": BoundedLevel.LOW,
        "coupling": BoundedLevel.LOW,
        "novelty": BoundedLevel.LOW,
        "expected_file_count": 2,
    }
    values.update(updates)
    return TaskClassification.model_validate(values)


def worker_context(content: bytes = b"relevant source") -> ContextManifest:
    return ContextManifest(
        role="worker",
        entries=(
            ContextEntry.from_content(
                source_type="repository_file",
                reference="src/example.py",
                content=content,
            ),
        ),
    )


def capsule(base: str, branch: str = "step09a/experiment/t1/grok-a") -> WorkerTaskCapsule:
    return WorkerTaskCapsule(
        experiment_id="step09a-test",
        task_id="t1",
        worker_profile="grok-subscription-a",
        base_commit=base,
        target_branch=branch,
        goal="Add a pure validator.",
        classification=classification(),
        predicted_mode=ExecutionMode.GROK_REVIEWED,
        actual_mode=ExecutionMode.GROK_REVIEWED,
        allowed_paths=("src/example.py", "tests/test_example.py"),
        acceptance_criteria=("Reject malformed input",),
        forbidden_changes=("Do not relax tests",),
        required_gates=(RequiredGate(name="focused", command_id="pytest-focused"),),
        maximum_execution_seconds=600,
        context_manifest=worker_context(),
    )


def usage() -> ReportedUsage:
    return ReportedUsage(input_tokens=100, output_tokens=20)


def seed_request() -> TrialSeedRequest:
    return TrialSeedRequest(
        experiment_id="step09a-test",
        task_id="t1",
        worker_profile="grok-subscription-a",
        project_id="vuzol",
        base_commit="a" * 40,
        goal="Add a pure validator.",
        classification=classification(),
        allowed_paths=("src/example.py", "tests/test_example.py"),
        acceptance_criteria=("Reject malformed input",),
        forbidden_changes=("Do not relax tests",),
        required_gates=(RequiredGate(name="focused", command_id="pytest-focused"),),
        context_manifest=worker_context(),
    )


def telemetry(**updates: object) -> ExperimentTelemetry:
    entry = ContextEntry.from_content(
        source_type="capsule", reference="capsule:t1", content=b"capsule"
    )
    invocation = InvocationTelemetry(
        role="worker",
        profile_id="grok-subscription-a",
        model="grok-build",
        context=ContextManifest(role="worker", entries=(entry,)),
        usage=usage(),
        duration_ms=10,
    )
    values: dict[str, object] = {
        "experiment_id": "step09a-test",
        "task_id": "t1",
        "task_class": TaskClass.PURE_MODEL_VALIDATOR,
        "predicted_mode": ExecutionMode.GROK_REVIEWED,
        "actual_mode": ExecutionMode.GROK_REVIEWED,
        "worker_profile": "grok-subscription-a",
        "reviewer_profile": "codex-subscription-prod",
        "base_commit": "a" * 40,
        "result_commit": "b" * 40,
        "allowed_paths": ("src/example.py",),
        "actual_changed_files": ("src/example.py",),
        "queue_wait_ms": 1,
        "execution_duration_ms": 10,
        "gate_duration_ms": 2,
        "review_duration_ms": 3,
        "repair_duration_ms": 0,
        "total_wall_time_ms": 16,
        "invocations": (invocation,),
        "estimated_cost": Decimal("0.01"),
        "pricing_revision": PricingRevision(
            revision="trial-v1",
            effective_at=datetime(2026, 7, 13, tzinfo=UTC),
            configured_cost_per_call=Decimal("0.01"),
        ),
        "worker_attempts": 1,
        "repair_count": 0,
        "repair_severity": RepairSeverity.NONE,
        "defect_categories": frozenset(),
        "final_outcome": ReviewOutcome.ACCEPTED_FIRST_PASS,
        "human_intervention": False,
        "shadow_would_accept": True,
        "shadow_decision_correct": True,
        "egress_unavailable_reason": "current proxy exposes no reliable per-run byte total",
        "environment_variable_names": ("HOME", "PATH"),
        "worker_mount_destinations": ("/workspace", "/artifacts", "/grok-home"),
        "network_policy_id": "grok-runtime-v1",
        "image_identity": "image@sha256:" + "c" * 64,
        "worktree_identity": "worktree:t1",
    }
    values.update(updates)
    return ExperimentTelemetry.model_validate(values)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ("git", "-C", str(repo), *args),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _mock_row(**attributes: object) -> MagicMock:
    row = MagicMock()
    for name, value in attributes.items():
        setattr(row, name, value)
    return row


def _scalar_result(rows: list[object]) -> MagicMock:
    result = MagicMock()
    result.all.return_value = rows
    return result
