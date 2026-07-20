"""Worker finalizer gates tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import (
    SYSTEM_GIT_CONFIG,
    Any,
    AsyncMock,
    CancellationContext,
    CapturedOutput,
    GateEvidence,
    GateRun,
    LocalGit,
    MagicMock,
    NormalizedUsage,
    Path,
    RequiredGate,
    TrustedGateRunner,
    VerificationResult,
    WorkerEditReport,
    WorkerFinalizationError,
    WorkerFinalizer,
    _edit_report,
    _finalizer_capsule,
    _finalizer_repository,
    _gate_context,
    _git,
    _normalized_usage,
    _reported_usage,
    _sandbox_gate_runner,
    hashlib,
    pytest,
    uuid,
)


@pytest.mark.anyio
async def test_worker_finalizer_measures_gates_and_creates_exactly_one_commit(
    tmp_path: Path,
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    hook_marker = tmp_path / "hook-ran"
    hook = repository / ".git" / "hooks" / "pre-commit"
    hook.write_text(f"#!/bin/sh\ntouch {hook_marker}\nexit 1\n")
    hook.chmod(0o700)

    async def fake_provider_edit(worktree: Path) -> WorkerEditReport:
        (worktree / "src" / "example.py").write_text("VALUE = 2\n")
        return _edit_report()

    edit_report = await fake_provider_edit(repository)

    artifacts = MagicMock()
    artifacts.persist = AsyncMock()
    gate_runner, envelopes, runtime = _sandbox_gate_runner()
    finalizer = WorkerFinalizer(LocalGit(), gate_runner=gate_runner, artifacts=artifacts)
    access = MagicMock()
    access.revoke = AsyncMock()
    result = await finalizer.finalize(
        worktree=repository,
        capsule=_finalizer_capsule(base, branch),
        edit_report=edit_report,
        worker_profile="grok-a",
        provider_usage=_normalized_usage(),
        provider_attempt=1,
        gate_context=_gate_context(),
        cancellation=CancellationContext(),
        access=access,
    )

    manifest = result.manifest
    assert manifest.experiment_id == "step09a-finalizer-test"
    assert manifest.task_id == "bounded-edit"
    assert manifest.claimed_complete is True
    assert manifest.changed_files == ("src/example.py",)
    assert manifest.usage.input_tokens == 11
    assert manifest.usage.cached_input_tokens == 3
    assert manifest.usage.output_tokens == 7
    assert manifest.result_commit == _git(repository, "rev-parse", "HEAD").strip()
    assert _git(repository, "rev-parse", f"{manifest.result_commit}^").strip() == base
    assert _git(repository, "rev-list", "--count", f"{base}..HEAD").strip() == "1"
    assert _git(repository, "status", "--short") == ""
    assert _git(repository, "show", "-s", "--format=%an <%ae>").strip() == (
        "Vuzol Worker Finalizer <vuzol-worker@localhost.invalid>"
    )
    assert not hook_marker.exists()
    assert "core.hooksPath=/dev/null" in SYSTEM_GIT_CONFIG
    assert "credential.helper=" in SYSTEM_GIT_CONFIG
    assert "commit.gpgSign=false" in SYSTEM_GIT_CONFIG
    assert result.evidence.verification is not None
    assert result.evidence.verification.passed
    assert [gate.command_id for gate in manifest.gates] == [
        "make test",
        "make format-check",
        "make lint",
        "make type-check",
    ]
    assert all(run.evidence.argv[0] == "/usr/bin/make" for run in result.gate_runs)
    assert result.evidence.canonicalization is not None
    assert result.evidence.canonicalization.input_files == ("src/example.py",)
    envelopes.build_canonicalizer.assert_awaited_once()
    assert envelopes.build_gate.await_count == 4
    assert runtime.run.await_count == 5
    access.revoke.assert_awaited_once()

    await finalizer.persist(
        AsyncMock(),
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        result=result,
    )
    artifact_types = [call.kwargs["artifact_type"] for call in artifacts.persist.await_args_list]
    assert "provider_edit_report" in artifact_types
    assert "worker_finalization_evidence" in artifact_types
    assert len([item for item in artifact_types if item.endswith("_stdout")]) == 4
    assert len([item for item in artifact_types if item.endswith("_stderr")]) == 4
    await WorkerFinalizer(LocalGit(), gate_runner=gate_runner).persist(
        AsyncMock(),
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        result=result,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("edit_path", "expected_category"),
    ((None, "worker_empty_change"), ("outside.txt", "worker_scope_violation")),
)
async def test_worker_finalizer_rejects_empty_or_out_of_scope_before_gates(
    tmp_path: Path, edit_path: str | None, expected_category: str
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    if edit_path is not None:
        (repository / edit_path).write_text("outside\n")
    gates = MagicMock()
    gates.run = AsyncMock()
    finalizer = WorkerFinalizer(LocalGit(), gate_runner=gates)
    with pytest.raises(WorkerFinalizationError) as captured:
        await finalizer.finalize(
            worktree=repository,
            capsule=_finalizer_capsule(base, branch),
            edit_report=_edit_report(),
            worker_profile="grok-a",
            provider_usage=_normalized_usage(),
            provider_attempt=1,
        )
    assert captured.value.category == expected_category
    gates.run.assert_not_awaited()
    assert _git(repository, "rev-parse", "HEAD").strip() == base


@pytest.mark.anyio
async def test_failed_gate_prevents_system_commit_and_retains_measured_evidence(
    tmp_path: Path,
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    (repository / "src" / "example.py").write_text("VALUE = 2\n")
    empty = CapturedOutput(
        content=b"", sha256=hashlib.sha256(b"").hexdigest(), byte_count=0, truncated=False
    )
    gate = GateRun(
        evidence=GateEvidence(
            name="test",
            command_id="make test",
            argv=("/usr/bin/make", "test"),
            exit_code=2,
            duration_ms=4,
            stdout_sha256=empty.sha256,
            stdout_bytes=0,
            stdout_truncated=False,
            stderr_sha256=empty.sha256,
            stderr_bytes=0,
            stderr_truncated=False,
        ),
        stdout=empty,
        stderr=empty,
    )
    gates = MagicMock()
    gates.run = AsyncMock(return_value=(gate,))
    access = MagicMock()
    access.revoke = AsyncMock()
    capsule = _finalizer_capsule(
        base,
        branch,
        gates=(RequiredGate(name="test", command_id="make test"),),
    )
    with pytest.raises(WorkerFinalizationError) as captured:
        await WorkerFinalizer(LocalGit(), gate_runner=gates).finalize(
            worktree=repository,
            capsule=capsule,
            edit_report=_edit_report(),
            worker_profile="grok-a",
            provider_usage=_normalized_usage(),
            provider_attempt=1,
            access=access,
        )
    assert captured.value.category == "worker_gate_failed"
    assert captured.value.result.evidence.gates[0].exit_code == 2
    assert captured.value.result.gate_runs == (gate,)
    assert _git(repository, "rev-parse", "HEAD").strip() == base
    assert "src/example.py" in _git(repository, "status", "--short")
    access.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_trusted_gate_registry_rejects_arbitrary_text_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    execute = AsyncMock()
    monkeypatch.setattr(TrustedGateRunner, "_execute", execute)
    runner, _envelopes, _runtime = _sandbox_gate_runner()
    with pytest.raises(ValueError, match="unknown trusted gate"):
        await runner.run(
            tmp_path,
            (RequiredGate(name="unsafe", command_id="make test && git commit -am bad"),),
            timeout_seconds=10,
            context=_gate_context(),
            cancellation=CancellationContext(),
        )
    execute.assert_not_awaited()


@pytest.mark.anyio
async def test_trusted_gate_registry_resolves_offline_security_preflight(
    tmp_path: Path,
) -> None:
    runner, envelopes, runtime = _sandbox_gate_runner()
    result = await runner.run(
        tmp_path,
        (RequiredGate(name="security", command_id="make security"),),
        timeout_seconds=10,
        context=_gate_context(),
        cancellation=CancellationContext(),
    )
    assert result[0].evidence.argv == ("/usr/bin/make", "security")
    assert result[0].evidence.exit_code == 0
    envelopes.build_gate.assert_awaited_once()
    runtime.run.assert_awaited_once()


@pytest.mark.anyio
async def test_provider_created_commit_is_rejected_before_deterministic_gates(
    tmp_path: Path,
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    (repository / "src" / "example.py").write_text("VALUE = 2\n")
    _git(repository, "add", "src/example.py")
    _git(
        repository,
        "-c",
        "user.name=Provider",
        "-c",
        "user.email=provider@example.invalid",
        "commit",
        "-m",
        "provider commit",
    )
    gates = MagicMock()
    gates.run = AsyncMock()
    with pytest.raises(WorkerFinalizationError) as captured:
        await WorkerFinalizer(LocalGit(), gate_runner=gates).finalize(
            worktree=repository,
            capsule=_finalizer_capsule(base, branch, parent_attempt=1),
            edit_report=_edit_report(attempt=2, claimed_complete=True),
            worker_profile="grok-a",
            provider_usage=_normalized_usage(),
            provider_attempt=2,
        )
    assert captured.value.category == "worker_precommitted"
    gates.run.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("branch", "remote", "gate-runtime", "gate-registry"))
async def test_worker_finalizer_additional_fail_closed_boundaries(
    tmp_path: Path, failure: str
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    (repository / "src" / "example.py").write_text("VALUE = 4\n")
    capsule = _finalizer_capsule(base, "wrong-branch" if failure == "branch" else branch)
    gates = MagicMock()
    gates.run = AsyncMock()
    if failure == "remote":
        _git(repository, "remote", "add", "origin", str(tmp_path))
    if failure == "gate-runtime":
        gates.run.side_effect = RuntimeError("sandbox unavailable")
    if failure == "gate-registry":
        gates.run.side_effect = ValueError("unknown trusted gate command ID")
    with pytest.raises(WorkerFinalizationError) as captured:
        await WorkerFinalizer(LocalGit(), gate_runner=gates).finalize(
            worktree=repository,
            capsule=capsule,
            edit_report=_edit_report(),
            worker_profile="grok-a",
            provider_usage=_normalized_usage(),
            provider_attempt=1,
        )
    expected = {
        "branch": "worker_branch_mismatch",
        "remote": "worker_git_isolation",
        "gate-runtime": "worker_gate_execution",
        "gate-registry": "worker_gate_registry",
    }
    assert captured.value.category == expected[failure]
    with pytest.raises(RuntimeError, match="no result manifest"):
        _ = captured.value.result.manifest


@pytest.mark.anyio
async def test_successful_gate_that_mutates_patch_prevents_commit(tmp_path: Path) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    source = repository / "src" / "example.py"
    source.write_text("VALUE = 2\n")
    empty = CapturedOutput(
        content=b"", sha256=hashlib.sha256(b"").hexdigest(), byte_count=0, truncated=False
    )
    gate = GateRun(
        evidence=GateEvidence(
            name="test",
            command_id="make test",
            argv=("/usr/bin/make", "test"),
            exit_code=0,
            duration_ms=1,
            stdout_sha256=empty.sha256,
            stdout_bytes=0,
            stdout_truncated=False,
            stderr_sha256=empty.sha256,
            stderr_bytes=0,
            stderr_truncated=False,
        ),
        stdout=empty,
        stderr=empty,
    )

    async def mutate(*_args: Any, **_kwargs: Any) -> tuple[GateRun, ...]:
        source.write_text("VALUE = 999\n")
        return (gate,)

    gates = MagicMock()
    gates.run = AsyncMock(side_effect=mutate)
    capsule = _finalizer_capsule(
        base,
        branch,
        gates=(RequiredGate(name="test", command_id="make test"),),
    )
    with pytest.raises(WorkerFinalizationError) as captured:
        await WorkerFinalizer(LocalGit(), gate_runner=gates).finalize(
            worktree=repository,
            capsule=capsule,
            edit_report=_edit_report(),
            worker_profile="grok-a",
            provider_usage=_normalized_usage(),
            provider_attempt=1,
        )
    assert captured.value.category == "worker_gate_mutated_worktree"
    assert _git(repository, "rev-parse", "HEAD").strip() == base


def test_finalizer_marks_fully_unavailable_provider_usage_without_inventing_tokens() -> None:
    usage = _reported_usage(NormalizedUsage(duration_ms=1))
    assert usage.input_tokens is None
    assert usage.cached_input_tokens is None
    assert usage.output_tokens is None
    assert usage.reasoning_tokens is None
    assert usage.unavailable_reason is not None


@pytest.mark.anyio
async def test_linked_repair_attempt_uses_deterministic_finalization_path(
    tmp_path: Path,
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    (repository / "src" / "example.py").write_text("VALUE = 3\n")
    runner, _envelopes, _runtime = _sandbox_gate_runner()
    result = await WorkerFinalizer(LocalGit(), gate_runner=runner).finalize(
        worktree=repository,
        capsule=_finalizer_capsule(base, branch, parent_attempt=1),
        edit_report=_edit_report(attempt=2, claimed_complete=True),
        worker_profile="grok-a",
        provider_usage=_normalized_usage(),
        provider_attempt=2,
        gate_context=_gate_context(),
        cancellation=CancellationContext(),
    )
    assert result.manifest.attempt == 2
    assert result.manifest.result_commit == _git(repository, "rev-parse", "HEAD").strip()
    assert result.evidence.verification is not None
    assert result.evidence.verification.passed


@pytest.mark.anyio
async def test_generated_manifest_verifier_failure_is_retained_and_rejected(
    tmp_path: Path,
) -> None:
    repository, base, branch = _finalizer_repository(tmp_path)
    (repository / "src" / "example.py").write_text("VALUE = 5\n")
    runner, _envelopes, _runtime = _sandbox_gate_runner()
    verifier = MagicMock()
    verifier.verify.return_value = VerificationResult(
        exact_base=True,
        exact_branch=True,
        commit_exists=True,
        changed_files_match=True,
        allowed_scope=True,
        gates_match=False,
        findings=("required successful gate evidence is missing",),
    )
    with pytest.raises(WorkerFinalizationError) as captured:
        await WorkerFinalizer(LocalGit(), gate_runner=runner, verifier=verifier).finalize(
            worktree=repository,
            capsule=_finalizer_capsule(base, branch),
            edit_report=_edit_report(),
            worker_profile="grok-a",
            provider_usage=_normalized_usage(),
            provider_attempt=1,
            gate_context=_gate_context(),
            cancellation=CancellationContext(),
        )
    assert captured.value.category == "worker_verification_failed"
    assert captured.value.result.evidence.verification == verifier.verify.return_value
    assert captured.value.result.evidence.result_manifest is not None
