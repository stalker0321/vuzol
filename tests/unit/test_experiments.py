import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.cli.experiment import _write_csv
from vuzol.experiments.domain import (
    BoundedLevel,
    ContextEntry,
    ContextManifest,
    DefectCategory,
    ExecutionMode,
    ExperimentTelemetry,
    GateResult,
    InvocationTelemetry,
    PricingRevision,
    RepairSeverity,
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
    assert "Exact base SHA: " + "a" * 40 in prompt
    assert "Stop on any mismatch" in prompt
    assert "Do not touch another VPS project" in prompt
    assert "one focused commit" in prompt
    assert "step09a-worker-result.v1" in prompt
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


def test_stable_experiment_identity_and_empty_context_ratio() -> None:
    trial = telemetry(invocations=())
    assert trial.repeated_context_ratio == 0.0
    assert stable_json_hash(trial) == stable_json_hash(trial)
    assert new_experiment_id().startswith("step09a-")
    with pytest.raises(ValidationError, match="unavailable cost"):
        telemetry(estimated_cost=None, cost_unavailable_reason=None)
    with pytest.raises(ValidationError, match="unavailable egress"):
        telemetry(egress_bytes=None, egress_unavailable_reason=None)


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
