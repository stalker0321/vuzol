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


def test_trial_seed_request_accepts_bounded_telegram_source_metadata() -> None:
    request = seed_request().model_copy(
        update={
            "source_user_id": 42,
            "source_chat_id": -100,
            "source_thread_id": 11,
        }
    )
    validated = TrialSeedRequest.model_validate(request.model_dump(mode="json"))
    assert validated.source_user_id == 42
    assert validated.source_chat_id == -100
    assert validated.source_thread_id == 11


def test_bounded_repair_context_accepts_only_measured_code_evidence() -> None:
    repair = BoundedRepairContext(
        current_diff="diff --git a/src/example.py b/src/example.py\n+VALUE = 2\n",
        changed_files=("src/example.py",),
        failed_gates=(
            RepairGateDiagnostic(
                command_id="make type-check",
                exit_code=2,
                sanitized_output="src/example.py:10: incompatible assignment",
            ),
        ),
        required_symbols=(
            RepairSymbolContext(reference="src/example.py:1-20", content="def example(): ..."),
        ),
    )
    request = seed_request().model_copy(update={"attempt": 2, "repair_context": repair})
    validated = TrialSeedRequest.model_validate(request.model_dump(mode="json"))
    assert validated.repair_context == repair
    assert validated.repair_context.changed_files == ("src/example.py",)


def test_repair_context_rejects_oversized_or_operational_history() -> None:
    with pytest.raises(ValidationError, match="prohibited operational history"):
        BoundedRepairContext(
            current_diff="private handoff contents",
            changed_files=("src/example.py",),
            failed_gates=(
                RepairGateDiagnostic(
                    command_id="make lint", exit_code=1, sanitized_output="failure"
                ),
            ),
        )
    with pytest.raises(ValidationError, match="linked repair requires"):
        TrialSeedRequest.model_validate(
            seed_request().model_copy(update={"attempt": 2}).model_dump(mode="json")
        )


def test_capsule_is_immutable_versioned_and_rejects_secrets() -> None:
    item = capsule("a" * 40)
    assert item.schema_version == "step09a-task-capsule.v1"
    with pytest.raises(ValidationError):
        item.goal = "changed"
    with pytest.raises(ValidationError, match="prohibited"):
        capsule("a" * 40).model_copy(
            update={"goal": "Read auth.json"},
        ).model_validate(capsule("a" * 40).model_dump() | {"goal": "Read auth.json"})


def test_worker_prompt_contains_exact_boundary_and_structured_result_requirement() -> None:
    prompt = render_worker_prompt(capsule("a" * 40), repository_id="vuzol")
    assert "Sandbox worktree: /workspace" in prompt
    assert "/workspace is writable" in prompt
    assert "Exact base SHA: " + "a" * 40 in prompt
    assert "Vuzol has already prepared and verified" in prompt
    assert "shell-backed repository tools" in prompt
    assert "read files, search repository contents" in prompt
    assert "create and edit ordinary files" in prompt
    assert "inspect the result of your edits without Git" in prompt
    assert "Do not invoke Git, shell commands" not in prompt
    assert "Do not touch another VPS project" in prompt
    assert "Do not invoke any Git command" in prompt
    assert "read or write .git" in prompt
    assert "Do not run required gates or tests" in prompt
    assert "install packages" in prompt
    assert "synchronize dependencies" in prompt
    assert "access the network" in prompt
    assert "access paths outside /workspace" in prompt
    assert "Vuzol will inspect the real diff" in prompt
    assert "run trusted gates, stage exact paths, create the commit" in prompt
    assert "authoritative result manifest" in prompt
    assert "actually inspect and edit those files" in prompt
    assert "claimed_complete=true only after making the intended changes" in prompt
    assert "claimed_complete=false only for a genuine inability" in prompt
    assert "lack of permission to run Git or tests does not prevent" in prompt
    assert "step09a-worker-edit-report.v1" in prompt
    assert "Do not claim changed files" in prompt
    assert "gate results, branch identity, or a result commit" in prompt
    assert '"goal":"Add a pure validator."' in prompt
    assert '"allowed_paths":["src/example.py","tests/test_example.py"]' in prompt
    assert '"acceptance_criteria":["Reject malformed input"]' in prompt
    assert '"forbidden_changes":["Do not relax tests"]' in prompt
    assert "/home/vodkolyan" not in prompt


def test_trial_seed_request_bounds_repairs_and_context_role() -> None:
    request = seed_request()
    assert request.maximum_repair_count == 2
    with pytest.raises(ValidationError):
        TrialSeedRequest.model_validate(request.model_dump() | {"maximum_repair_count": 3})


def test_mode_policy_is_explicit_and_security_cannot_be_lowered() -> None:
    assert classify_execution_mode(classification()) is ExecutionMode.GROK_REVIEWED
    risky = classification(security_boundary=True)
    assert classify_execution_mode(risky) is ExecutionMode.SOL_SOLO
    assert (
        enforce_security_escalation(risky, ExecutionMode.GROK_GATED_SHADOW)
        is ExecutionMode.SOL_SOLO
    )
    assert (
        classify_execution_mode(classification(testability=BoundedLevel.LOW))
        is ExecutionMode.SOL_SOLO
    )
    assert (
        classify_execution_mode(classification(task_class=TaskClass.SECURITY))
        is ExecutionMode.SOL_SOLO
    )


def test_profile_pin_only_accepts_internal_versioned_route() -> None:
    run = Run(selected_route={"schema_version": "step09a-route.v1", "trusted_profile_id": "grok-a"})
    assert _trusted_profile_id(run) == "grok-a"
    run.selected_route = {"trusted_profile_id": "grok-a"}
    assert _trusted_profile_id(run) is None
    run.selected_route = {"schema_version": "step09a-route.v1", "trusted_profile_id": 7}
    assert _trusted_profile_id(run) is None


def test_context_hashing_and_repeated_measurement() -> None:
    original = ContextEntry.from_content(
        source_type="repository_file", reference="src/a.py", content=b"abc"
    )
    repeated = ContextEntry.from_content(
        source_type="repository_file",
        reference="src/a.py",
        content=b"abc",
        repeated_from_roles=("planner",),
    )
    assert original.content_hash == repeated.content_hash
    manifest = ContextManifest(role="worker", entries=(original, repeated))
    assert manifest.total_bytes == 6
    assert manifest.repeated_bytes == 3
    assert manifest.estimated_tokens == 2
    with pytest.raises(ValidationError, match="both endpoints"):
        ContextEntry(
            source_type="file",
            reference="a",
            content_hash="a" * 64,
            line_start=1,
            byte_count=0,
            estimated_tokens=0,
        )
    with pytest.raises(ValidationError, match="reversed"):
        ContextEntry(
            source_type="file",
            reference="a",
            content_hash="a" * 64,
            line_start=2,
            line_end=1,
            byte_count=0,
            estimated_tokens=0,
        )


def test_missing_usage_is_never_fabricated() -> None:
    missing = ReportedUsage(unavailable_reason="CLI did not expose structured usage")
    assert missing.input_tokens is None
    with pytest.raises(ValidationError, match="explanation"):
        ReportedUsage()


def test_outcome_and_repair_taxonomies_are_closed() -> None:
    assert ReviewOutcome("accepted_after_minor_repair") is ReviewOutcome.ACCEPTED_AFTER_MINOR_REPAIR
    assert RepairSeverity("major") is RepairSeverity.MAJOR
    with pytest.raises(ValueError):
        ReviewOutcome("mostly_ok")


def test_capsule_repair_limit_and_override_reason() -> None:
    data = capsule("a" * 40).model_dump()
    with pytest.raises(ValidationError):
        WorkerTaskCapsule.model_validate(data | {"maximum_repair_count": 3})
    with pytest.raises(ValidationError, match="override"):
        WorkerTaskCapsule.model_validate(
            data | {"actual_mode": ExecutionMode.SOL_SOLO, "override_reason": None}
        )
    with pytest.raises(ValidationError, match="repository-relative"):
        WorkerTaskCapsule.model_validate(data | {"allowed_paths": ("/etc/passwd",)})
    wrong_context = ContextManifest(role="reviewer")
    with pytest.raises(ValidationError, match="worker context"):
        WorkerTaskCapsule.model_validate(data | {"context_manifest": wrong_context})


def test_scope_conflict_and_allowed_file_enforcement() -> None:
    assert scopes_conflict(("src/a",), ("src/a/file.py",))
    assert not scopes_conflict(("src/a.py",), ("docs/b.md",))
    assert path_is_allowed("src/a/file.py", ("src/a",))
    assert not path_is_allowed("src/ab/file.py", ("src/a",))
    assert not path_is_allowed("../secret", ("src",))


def test_suspicious_pattern_report_has_locations_and_classifications() -> None:
    findings = scan_suspicious_patterns(
        {
            "tests/test_bad.py": "def test_bad():\n    assert call() or True\n",
            "src/bad.py": "try:\n    work()\nexcept Exception: pass\n",
        }
    )
    assert {(item.path, item.line, item.classification) for item in findings} == {
        ("tests/test_bad.py", 2, "forced_success"),
        ("src/bad.py", 3, "exception_swallowing"),
    }


def test_shadow_auto_accept_and_false_accept_aggregation() -> None:
    verified = VerificationResult(
        exact_base=True,
        exact_branch=True,
        commit_exists=True,
        changed_files_match=True,
        allowed_scope=True,
        gates_match=True,
    )
    assert shadow_auto_accept(verified, (), diff_lines=20, changed_file_count=2)
    assert not shadow_auto_accept(verified, (), diff_lines=900, changed_file_count=2)
    trial = telemetry(shadow_would_accept=True, shadow_decision_correct=False)
    summary = aggregate_trials((trial,))
    assert summary["shadow_false_accepts"] == 1
    assert summary["shadow_false_rejects"] == 0


def test_worker_result_is_verified_against_real_git(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(("git", "init", "-b", "main", str(repo)), check=True, capture_output=True)
    subprocess.run(
        ("git", "-C", str(repo), "config", "user.email", "test@example.invalid"), check=True
    )
    subprocess.run(("git", "-C", str(repo), "config", "user.name", "Test"), check=True)
    source = repo / "src"
    source.mkdir()
    (source / "example.py").write_text("BASE = True\n")
    subprocess.run(("git", "-C", str(repo), "add", "."), check=True)
    subprocess.run(
        ("git", "-C", str(repo), "commit", "-m", "base"), check=True, capture_output=True
    )
    base = git(repo, "rev-parse", "HEAD")
    branch = "step09a/experiment/t1/grok-a"
    subprocess.run(
        ("git", "-C", str(repo), "switch", "-c", branch), check=True, capture_output=True
    )
    (source / "example.py").write_text("BASE = False\n")
    subprocess.run(
        ("git", "-C", str(repo), "commit", "-am", "change"), check=True, capture_output=True
    )
    result = git(repo, "rev-parse", "HEAD")
    manifest = WorkerResultManifest(
        experiment_id="step09a-test",
        task_id="t1",
        worker_profile="grok-subscription-a",
        base_commit=base,
        result_commit=result,
        branch=branch,
        changed_files=("src/example.py",),
        claimed_complete=True,
        gates=(
            GateResult(name="focused", command_id="pytest-focused", exit_code=0, duration_ms=1),
        ),
        total_worker_duration_ms=10,
        usage=usage(),
    )
    verified = GitWorkerResultVerifier().verify(repo, capsule(base, branch), manifest)
    assert verified.passed
    assert not verified.findings
    stale = manifest.model_copy(update={"result_commit": base, "changed_files": ()})
    stale_verification = GitWorkerResultVerifier().verify(repo, capsule(base, branch), stale)
    assert not stale_verification.commit_exists
    assert "worktree HEAD differs from result commit" in stale_verification.findings


def test_result_verification_rejects_false_gate_and_scope_claim(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(("git", "init", "-b", "main", str(repo)), check=True, capture_output=True)
    subprocess.run(
        ("git", "-C", str(repo), "config", "user.email", "test@example.invalid"), check=True
    )
    subprocess.run(("git", "-C", str(repo), "config", "user.name", "Test"), check=True)
    (repo / "README.md").write_text("base\n")
    subprocess.run(("git", "-C", str(repo), "add", "."), check=True)
    subprocess.run(
        ("git", "-C", str(repo), "commit", "-m", "base"), check=True, capture_output=True
    )
    base = git(repo, "rev-parse", "HEAD")
    branch = "step09a/experiment/t1/grok-a"
    subprocess.run(
        ("git", "-C", str(repo), "switch", "-c", branch), check=True, capture_output=True
    )
    (repo / "README.md").write_text("changed\n")
    subprocess.run(
        ("git", "-C", str(repo), "commit", "-am", "bad scope"), check=True, capture_output=True
    )
    result = git(repo, "rev-parse", "HEAD")
    manifest = WorkerResultManifest(
        experiment_id="step09a-test",
        task_id="t1",
        worker_profile="grok-subscription-a",
        base_commit=base,
        result_commit=result,
        branch=branch,
        changed_files=("src/example.py",),
        claimed_complete=True,
        gates=(
            GateResult(name="focused", command_id="pytest-focused", exit_code=1, duration_ms=1),
        ),
        total_worker_duration_ms=10,
        usage=usage(),
    )
    verified = GitWorkerResultVerifier().verify(repo, capsule(base, branch), manifest)
    assert not verified.passed
    assert not verified.changed_files_match
    assert not verified.allowed_scope
    assert not verified.gates_match


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


def test_telemetry_aggregation_pricing_revision_and_secret_rejection() -> None:
    first = telemetry()
    repaired = telemetry(
        task_id="t2",
        worker_attempts=2,
        repair_count=1,
        repair_severity=RepairSeverity.MINOR,
        defect_categories=frozenset({DefectCategory.MISSING_EDGE_CASE}),
        final_outcome=ReviewOutcome.ACCEPTED_AFTER_MINOR_REPAIR,
    )
    summary = aggregate_trials((first, repaired))
    assert summary["task_count"] == 2
    assert summary["provider_input_tokens"] == 200
    assert summary["provider_output_tokens"] == 40
    assert summary["accepted_first_pass"] == 1
    assert first.pricing_revision is not None
    assert first.pricing_revision.revision == "trial-v1"
    with pytest.raises(ValidationError, match="sensitive environment"):
        telemetry(environment_variable_names=("DATABASE_URL",))
    with pytest.raises(ValidationError, match="lineage"):
        telemetry(worker_attempts=1, repair_count=1)


def test_machine_readable_csv_export_preserves_measured_fields(tmp_path: Path) -> None:
    trial = telemetry()
    target = tmp_path / "results.csv"
    _write_csv(target, (trial,))
    lines = target.read_text().splitlines()
    assert "repeated_context_ratio" in lines[0]
    assert "step09a-test" in lines[1]
    assert "accepted_first_pass" in lines[1]
    assert "0.01" in lines[1]


def test_provider_usage_aggregates_by_role_without_fabricating_missing_values() -> None:
    context = ContextManifest(role="worker")
    invocations = (
        InvocationTelemetry(
            role="planner",
            profile_id="deterministic",
            model="deterministic",
            context=ContextManifest(role="planner"),
            usage=ReportedUsage(unavailable_reason="not a provider invocation"),
            duration_ms=1,
        ),
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=context,
            usage=ReportedUsage(
                input_tokens=100,
                cached_input_tokens=40,
                output_tokens=20,
                reasoning_tokens=5,
            ),
            duration_ms=2,
        ),
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=context,
            usage=ReportedUsage(
                input_tokens=0,
                cached_input_tokens=0,
                output_tokens=0,
                reasoning_tokens=0,
            ),
            duration_ms=3,
        ),
        InvocationTelemetry(
            role="reviewer",
            profile_id="codex-subscription-prod",
            model="codex",
            context=ContextManifest(role="reviewer"),
            usage=ReportedUsage(input_tokens=7, output_tokens=3, reasoning_tokens=2),
            duration_ms=4,
        ),
    )
    trial = telemetry(invocations=invocations)

    summary = aggregate_trials((trial,))

    assert summary["provider_input_tokens"] == 107
    assert summary["provider_output_tokens"] == 23
    assert summary["provider_cached_input_tokens"] == 40
    assert summary["provider_reasoning_tokens"] == 7
    assert summary["provider_usage_unavailable_invocations"] == 1
    assert summary["usage_by_role"] == {
        "planner": {
            "invocation_count": 1,
            "reported_invocation_count": 0,
            "unavailable_invocation_count": 1,
            "input_tokens": None,
            "cached_input_tokens": None,
            "output_tokens": None,
            "reasoning_tokens": None,
        },
        "worker": {
            "invocation_count": 2,
            "reported_invocation_count": 2,
            "unavailable_invocation_count": 0,
            "input_tokens": 100,
            "cached_input_tokens": 40,
            "output_tokens": 20,
            "reasoning_tokens": 5,
        },
        "reviewer": {
            "invocation_count": 1,
            "reported_invocation_count": 1,
            "unavailable_invocation_count": 0,
            "input_tokens": 7,
            "cached_input_tokens": None,
            "output_tokens": 3,
            "reasoning_tokens": 2,
        },
    }
    assert summary["task_count"] == 1
    assert summary["context_bytes"] == trial.total_context_bytes
    assert summary["accepted_first_pass"] == 1


def test_csv_role_usage_preserves_columns_and_zero_vs_unavailable(tmp_path: Path) -> None:
    invocations = (
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=ContextManifest(role="worker"),
            usage=ReportedUsage(
                input_tokens=0,
                cached_input_tokens=0,
                output_tokens=0,
                reasoning_tokens=0,
            ),
            duration_ms=1,
        ),
        InvocationTelemetry(
            role="worker",
            profile_id="grok-subscription-a",
            model="grok-build",
            context=ContextManifest(role="worker"),
            usage=ReportedUsage(input_tokens=4, cached_input_tokens=3, output_tokens=2),
            duration_ms=1,
        ),
        InvocationTelemetry(
            role="reviewer",
            profile_id="codex-session",
            model="codex",
            context=ContextManifest(role="reviewer"),
            usage=ReportedUsage(unavailable_reason="interactive usage unavailable"),
            duration_ms=1,
        ),
    )
    target = tmp_path / "role-usage.csv"

    _write_csv(target, (telemetry(invocations=invocations),))

    with target.open(newline="") as handle:
        reader = csv.DictReader(handle)
        row = next(reader)
        assert next(reader, None) is None
    existing_fields = [
        "experiment_id",
        "task_id",
        "task_class",
        "predicted_mode",
        "actual_mode",
        "worker_profile",
        "final_outcome",
        "worker_attempts",
        "repair_count",
        "repair_severity",
        "execution_duration_ms",
        "review_duration_ms",
        "total_wall_time_ms",
        "context_bytes",
        "repeated_context_bytes",
        "repeated_context_ratio",
        "shadow_would_accept",
        "shadow_decision_correct",
        "estimated_cost",
    ]
    assert reader.fieldnames is not None
    assert reader.fieldnames[: len(existing_fields)] == existing_fields
    assert reader.fieldnames[len(existing_fields) :] == [
        f"{role}_{field}"
        for role in ("planner", "worker", "reviewer")
        for field in (
            "invocation_count",
            "usage_unavailable_invocations",
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_tokens",
        )
    ]
    assert row["experiment_id"] == "step09a-test"
    assert row["final_outcome"] == "accepted_first_pass"
    assert row["planner_invocation_count"] == "0"
    assert row["planner_usage_unavailable_invocations"] == "0"
    assert row["planner_input_tokens"] == ""
    assert row["worker_invocation_count"] == "2"
    assert row["worker_usage_unavailable_invocations"] == "0"
    assert row["worker_input_tokens"] == "4"
    assert row["worker_cached_input_tokens"] == "3"
    assert row["worker_output_tokens"] == "2"
    assert row["worker_reasoning_tokens"] == "0"
    assert row["reviewer_invocation_count"] == "1"
    assert row["reviewer_usage_unavailable_invocations"] == "1"
    assert row["reviewer_input_tokens"] == ""
    assert row["reviewer_cached_input_tokens"] == ""
    assert row["reviewer_output_tokens"] == ""
    assert row["reviewer_reasoning_tokens"] == ""


def test_stable_experiment_identity_and_empty_context_ratio() -> None:
    trial = telemetry(invocations=())
    assert trial.repeated_context_ratio == 0.0
    assert stable_json_hash(trial) == stable_json_hash(trial)
    assert new_experiment_id().startswith("step09a-")
    with pytest.raises(ValidationError, match="unavailable cost"):
        telemetry(estimated_cost=None, cost_unavailable_reason=None)
    with pytest.raises(ValidationError, match="unavailable egress"):
        telemetry(egress_bytes=None, egress_unavailable_reason=None)


def test_step09a_execute_code_receives_non_authoritative_edit_report_schema() -> None:
    from vuzol.providers.handlers import _step09a_result_schema

    name, version, schema = _step09a_result_schema(
        "execute_code", {"step09a_capsule": {"schema_version": "step09a-task-capsule.v1"}}
    )
    assert name == "WorkerEditReport"
    assert version == "step09a-worker-edit-report.v1"
    assert schema is not None
    required = schema["required"]
    assert isinstance(required, list)
    assert set(required) >= {
        "experiment_id",
        "task_id",
        "claimed_complete",
        "implementation_summary",
    }
    properties = schema["properties"]
    assert isinstance(properties, dict)
    assert "attempt" in properties
    assert "result_commit" not in properties
    assert "changed_files" not in properties
    assert "gates" not in properties
    assert "branch" not in properties
    assert _step09a_result_schema("execute_code", {}) == (None, None, None)
    assert _step09a_result_schema("plan", {"step09a_capsule": {}}) == (None, None, None)


def test_no_automatic_merge_deploy_or_direct_grok_host_path_exists() -> None:
    package = Path(__file__).parents[2] / "src" / "vuzol" / "experiments"
    source = "\n".join(path.read_text() for path in package.glob("*.py"))
    assert "grok --" not in source
    assert "git merge" not in source
    assert "git push" not in source
    assert "systemctl" not in source


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


def test_inspect_serializers_expose_only_safe_process_and_artifact_fields() -> None:
    step_id = uuid.UUID(int=1)
    process_id = uuid.UUID(int=2)
    events_id = uuid.UUID(int=3)
    artifact_id = uuid.UUID(int=4)
    verified_at = datetime(2026, 7, 14, 12, 30, tzinfo=UTC)
    process = _mock_row(
        id=process_id,
        step_id=step_id,
        profile_id="grok-subscription-a",
        provider_attempt=2,
        status=ProcessStatus.EXITED,
        outcome=ProcessOutcome.SUCCEEDED,
        image_digest="image@sha256:" + "a" * 64,
        exit_code=0,
        runtime_metadata={"actual_elapsed_ms": 1250, "private": "do-not-expose"},
        provider_events_artifact_id=events_id,
        provider_result_artifact_id=None,
        command_envelope={"argv": ["secret"]},
        command_envelope_hash="b" * 64,
        container_id="private-container",
        host_pid=999,
        working_directory="/private/worktree",
    )
    artifact = _mock_row(
        id=artifact_id,
        step_id=step_id,
        producer_process_id=process_id,
        artifact_type="provider-event-summary",
        size_bytes=42,
        content_hash="c" * 64,
        media_type="application/json",
        storage_state=ArtifactStorageState.AVAILABLE,
        verified_at=verified_at,
        content_uri="artifact:private",
        storage_key="private-key",
        metadata_json={"private": True},
        retention_until=verified_at,
    )

    rendered_process = _serialize_process(process)
    rendered_artifact = _serialize_artifact(artifact)

    assert rendered_process == {
        "process_uuid": str(process_id),
        "step_uuid": str(step_id),
        "profile_id": "grok-subscription-a",
        "provider_attempt": 2,
        "status": "exited",
        "outcome": "succeeded",
        "image_digest": "image@sha256:" + "a" * 64,
        "exit_code": 0,
        "duration_ms": 1250,
        "provider_events_artifact_id": str(events_id),
        "provider_result_artifact_id": None,
    }
    assert rendered_artifact == {
        "artifact_uuid": str(artifact_id),
        "step_uuid": str(step_id),
        "producer_process_uuid": str(process_id),
        "type": "provider-event-summary",
        "size_bytes": 42,
        "content_hash": "c" * 64,
        "media_type": "application/json",
        "storage_state": "available",
        "verified_at": "2026-07-14T12:30:00+00:00",
    }


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        ({"actual_elapsed_ms": 0}, 0),
        ({"actual_elapsed_ms": 987}, 987),
        ({"actual_elapsed_ms": -1}, None),
        ({"actual_elapsed_ms": 1.5}, None),
        ({"actual_elapsed_ms": True}, None),
        ({"actual_elapsed_ms": "12"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_process_duration_requires_safe_non_negative_integer(
    metadata: object, expected: int | None
) -> None:
    process = _mock_row(
        id=uuid.UUID(int=1),
        step_id=uuid.UUID(int=2),
        profile_id="profile",
        provider_attempt=1,
        status=ProcessStatus.EXITED,
        outcome=None,
        image_digest="image@sha256:" + "a" * 64,
        exit_code=None,
        runtime_metadata=metadata,
        provider_events_artifact_id=None,
        provider_result_artifact_id=None,
    )

    assert _serialize_process(process)["duration_ms"] == expected


def test_inspect_latest_flag_is_optional() -> None:
    default = _parse_args(["inspect", "experiment"])
    latest = _parse_args(["inspect", "experiment", "--latest"])

    assert default.command == "inspect"
    assert default.experiment_id == "experiment"
    assert default.latest is False
    assert latest.latest is True


@pytest.mark.anyio
async def test_inspect_latest_selects_newest_with_deterministic_uuid_tie() -> None:
    experiment_id = "step09a-inspect-latest"
    created = datetime(2026, 7, 15, 12, tzinfo=UTC)
    older = _mock_row(
        id=uuid.UUID(int=40),
        task_id=uuid.UUID(int=140),
        workflow_type="adaptive_worker_trial",
        created_at=datetime(2026, 7, 15, 11, tzinfo=UTC),
        status=RunStatus.COMPLETED,
        selected_route={"experiment_id": experiment_id, "experiment_task_id": "older"},
    )
    tied_lower = _mock_row(
        id=uuid.UUID(int=41),
        task_id=uuid.UUID(int=141),
        workflow_type="adaptive_worker_trial",
        created_at=created,
        status=RunStatus.COMPLETED,
        selected_route={"experiment_id": experiment_id, "experiment_task_id": "lower"},
    )
    tied_higher = _mock_row(
        id=uuid.UUID(int=42),
        task_id=uuid.UUID(int=142),
        workflow_type="adaptive_worker_trial",
        created_at=created,
        status=RunStatus.FAILED,
        selected_route={"experiment_id": experiment_id, "experiment_task_id": "higher"},
    )
    foreign = _mock_row(
        id=uuid.UUID(int=99),
        task_id=uuid.UUID(int=199),
        workflow_type="adaptive_worker_trial",
        created_at=datetime(2026, 7, 15, 13, tzinfo=UTC),
        status=RunStatus.COMPLETED,
        selected_route={"experiment_id": "another-experiment"},
    )
    runs: list[object] = [older, tied_lower, tied_higher, foreign]

    def factory_for(selected_runs: list[object]) -> MagicMock:
        session = AsyncMock(spec=AsyncSession)
        session.scalars.side_effect = [
            _scalar_result(selected_runs),
            *(_scalar_result([]) for _ in range(4 * len(selected_runs))),
        ]
        session.scalar.return_value = None
        context = AsyncMock()
        context.__aenter__.return_value = session
        context.__aexit__.return_value = False
        factory = MagicMock(spec=async_sessionmaker)
        factory.return_value = context
        return factory

    default = await _inspect(factory_for(runs), experiment_id)
    latest = await _inspect(factory_for(runs), experiment_id, latest=True)
    missing = await _inspect(factory_for([]), "missing", latest=True)

    assert [run["task_id"] for run in default["runs"]] == ["older", "lower", "higher"]
    assert [run["task_id"] for run in latest["runs"]] == ["higher"]
    assert latest["runs"][0]["run_uuid"] == str(tied_higher.id)
    assert missing == {"experiment_id": "missing", "runs": []}


@pytest.mark.anyio
async def test_inspect_preserves_fields_orders_evidence_and_excludes_foreign_rows() -> None:
    experiment_id = "step09a-inspect-safe"
    run_id = uuid.UUID(int=10)
    foreign_run_id = uuid.UUID(int=11)
    task_id = uuid.UUID(int=12)
    step_first_id = uuid.UUID(int=13)
    step_second_id = uuid.UUID(int=14)
    process_first_id = uuid.UUID(int=15)
    process_second_id = uuid.UUID(int=16)
    artifact_first_id = uuid.UUID(int=17)
    artifact_second_id = uuid.UUID(int=18)
    created = datetime(2026, 7, 14, 12, tzinfo=UTC)
    later = datetime(2026, 7, 14, 13, tzinfo=UTC)
    run = _mock_row(
        id=run_id,
        task_id=task_id,
        workflow_type="adaptive_worker_trial",
        created_at=created,
        status=RunStatus.COMPLETED,
        selected_route={
            "experiment_id": experiment_id,
            "experiment_task_id": "inspect-safe",
            "trusted_profile_id": "grok-subscription-a",
        },
    )
    foreign_run = _mock_row(
        id=foreign_run_id,
        task_id=uuid.UUID(int=19),
        workflow_type="adaptive_worker_trial",
        created_at=later,
        status=RunStatus.FAILED,
        selected_route={"experiment_id": "another-experiment"},
    )
    step_second = _mock_row(
        id=step_second_id,
        run_id=run_id,
        ordinal=2,
        step_type="execute_code",
        status=StepStatus.COMPLETED,
        attempt_count=1,
        failure_category=None,
    )
    step_first = _mock_row(
        id=step_first_id,
        run_id=run_id,
        ordinal=1,
        step_type="prepare_worktree",
        status=StepStatus.COMPLETED,
        attempt_count=1,
        failure_category=None,
    )
    foreign_step = _mock_row(
        id=uuid.UUID(int=20),
        run_id=foreign_run_id,
        ordinal=0,
        step_type="foreign",
        status=StepStatus.FAILED,
        attempt_count=1,
        failure_category="foreign",
    )
    patch_id = uuid.UUID(int=21)
    worktree = _mock_row(
        run_id=run_id,
        branch="vuzol/task-inspect",
        base_commit="a" * 40,
        result_commit="b" * 40,
        delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
        diff_hash="c" * 64,
        patch_artifact_id=patch_id,
        changed_files_artifact_id=None,
    )
    usage = _mock_row(
        run_id=run_id,
        profile_id="grok-subscription-a",
        model="grok-build",
        input_tokens=10,
        cached_tokens=20,
        output_tokens=30,
        duration_ms=40,
        cost_units=Decimal("0.010000"),
    )
    foreign_usage = _mock_row(run_id=foreign_run_id)
    process_second = _mock_row(
        id=process_second_id,
        run_id=run_id,
        step_id=step_second_id,
        created_at=later,
        profile_id="grok-subscription-a",
        provider_attempt=2,
        status=ProcessStatus.EXITED,
        outcome=None,
        image_digest="image@sha256:" + "d" * 64,
        exit_code=None,
        runtime_metadata={},
        provider_events_artifact_id=None,
        provider_result_artifact_id=None,
    )
    process_first = _mock_row(
        id=process_first_id,
        run_id=run_id,
        step_id=step_second_id,
        created_at=created,
        profile_id="grok-subscription-a",
        provider_attempt=1,
        status=ProcessStatus.EXITED,
        outcome=ProcessOutcome.SUCCEEDED,
        image_digest="image@sha256:" + "d" * 64,
        exit_code=0,
        runtime_metadata={"actual_elapsed_ms": 500},
        provider_events_artifact_id=uuid.UUID(int=22),
        provider_result_artifact_id=uuid.UUID(int=23),
    )
    foreign_process = _mock_row(id=uuid.UUID(int=24), run_id=foreign_run_id, created_at=created)
    artifact_second = _mock_row(
        id=artifact_second_id,
        run_id=run_id,
        step_id=step_second_id,
        producer_process_id=process_first_id,
        created_at=later,
        artifact_type="worker_finalization_evidence",
        size_bytes=200,
        content_hash="e" * 64,
        media_type="application/json",
        storage_state=ArtifactStorageState.AVAILABLE,
        verified_at=None,
    )
    artifact_first = _mock_row(
        id=artifact_first_id,
        run_id=run_id,
        step_id=step_second_id,
        producer_process_id=process_first_id,
        created_at=created,
        artifact_type="provider-event-summary",
        size_bytes=100,
        content_hash="f" * 64,
        media_type="application/json",
        storage_state=ArtifactStorageState.AVAILABLE,
        verified_at=created,
    )
    foreign_artifact = _mock_row(id=uuid.UUID(int=25), run_id=foreign_run_id, created_at=created)
    session = AsyncMock(spec=AsyncSession)
    session.scalars.side_effect = [
        _scalar_result([run, foreign_run]),
        _scalar_result([step_second, foreign_step, step_first]),
        _scalar_result([foreign_usage, usage]),
        _scalar_result([process_second, foreign_process, process_first]),
        _scalar_result([artifact_second, foreign_artifact, artifact_first]),
    ]
    session.scalar.return_value = worktree
    context = AsyncMock()
    context.__aenter__.return_value = session
    context.__aexit__.return_value = False
    factory = MagicMock(spec=async_sessionmaker)
    factory.return_value = context

    output = await _inspect(factory, experiment_id)

    assert output["experiment_id"] == experiment_id
    assert len(output["runs"]) == 1
    rendered = output["runs"][0]
    assert set(rendered) == {
        "task_id",
        "task_uuid",
        "run_uuid",
        "status",
        "profile_id",
        "steps",
        "worktree",
        "usage",
        "processes",
        "artifacts",
    }
    assert rendered["task_id"] == "inspect-safe"
    assert rendered["task_uuid"] == str(task_id)
    assert rendered["run_uuid"] == str(run_id)
    assert rendered["status"] == "completed"
    assert rendered["profile_id"] == "grok-subscription-a"
    assert [item["step_uuid"] for item in rendered["steps"]] == [
        str(step_first_id),
        str(step_second_id),
    ]
    assert [item["ordinal"] for item in rendered["steps"]] == [1, 2]
    assert set(rendered["steps"][0]) == {
        "step_uuid",
        "ordinal",
        "type",
        "status",
        "attempt_count",
        "failure_category",
    }
    assert rendered["worktree"] == {
        "branch": "vuzol/task-inspect",
        "base_commit": "a" * 40,
        "result_commit": "b" * 40,
        "delivery_state": "worktree_retained",
        "diff_hash": "c" * 64,
        "patch_artifact_id": str(patch_id),
        "changed_files_artifact_id": None,
    }
    assert rendered["usage"] == [
        {
            "profile_id": "grok-subscription-a",
            "model": "grok-build",
            "input_tokens": 10,
            "cached_tokens": 20,
            "output_tokens": 30,
            "duration_ms": 40,
            "cost_units": "0.010000",
        }
    ]
    assert [item["process_uuid"] for item in rendered["processes"]] == [
        str(process_first_id),
        str(process_second_id),
    ]
    assert rendered["processes"][0]["duration_ms"] == 500
    assert rendered["processes"][1]["duration_ms"] is None
    assert [item["artifact_uuid"] for item in rendered["artifacts"]] == [
        str(artifact_first_id),
        str(artifact_second_id),
    ]
    assert rendered["artifacts"][0]["verified_at"] == "2026-07-14T12:00:00+00:00"
    assert rendered["artifacts"][1]["verified_at"] is None
    statements = [str(call.args[0]) for call in session.scalars.await_args_list]
    assert "supervised_processes.run_id =" in statements[3]
    assert "artifacts.run_id =" in statements[4]
    serialized = json.dumps(output, sort_keys=True)
    for forbidden in (
        "content_uri",
        "storage_key",
        "metadata_json",
        "command_envelope",
        "command_envelope_hash",
        "container_id",
        "host_pid",
        "working_directory",
        "runtime_metadata",
        "private-container",
        "/private/worktree",
    ):
        assert forbidden not in serialized


@pytest.mark.anyio
async def test_inspect_handles_missing_optional_evidence() -> None:
    experiment_id = "step09a-inspect-empty"
    run_id = uuid.UUID(int=30)
    run = _mock_row(
        id=run_id,
        task_id=uuid.UUID(int=31),
        workflow_type="adaptive_worker_trial",
        created_at=datetime(2026, 7, 14, tzinfo=UTC),
        status=RunStatus.FAILED,
        selected_route={
            "experiment_id": experiment_id,
            "experiment_task_id": "inspect-empty",
            "trusted_profile_id": "grok-subscription-a",
        },
    )
    session = AsyncMock(spec=AsyncSession)
    session.scalars.side_effect = [
        _scalar_result([run]),
        _scalar_result([]),
        _scalar_result([]),
        _scalar_result([]),
        _scalar_result([]),
    ]
    session.scalar.return_value = None
    context = AsyncMock()
    context.__aenter__.return_value = session
    context.__aexit__.return_value = False
    factory = MagicMock(spec=async_sessionmaker)
    factory.return_value = context

    output = await _inspect(factory, experiment_id)

    rendered = output["runs"][0]
    assert rendered["steps"] == []
    assert rendered["worktree"] is None
    assert rendered["usage"] == []
    assert rendered["processes"] == []
    assert rendered["artifacts"] == []
