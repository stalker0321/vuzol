"""Verification tests (split for cohesion)."""

from __future__ import annotations

from ._test_experiments_helpers import *


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
