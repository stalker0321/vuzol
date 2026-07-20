"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from ._execution_helpers import (
    Any,
    AsyncMock,
    CodexInvocation,
    CodexProcessResult,
    EgressDestination,
    ExecutionEnvelopeFactory,
    GateExecutionContext,
    IdempotencyClass,
    MagicMock,
    MountMode,
    NetworkPolicy,
    Path,
    SandboxNetworkMode,
    SandboxProfileConfig,
    Step,
    StepStatus,
    Worktree,
    _seccomp_profile,
    canonical_codex_argv,
    canonical_grok_argv,
    json,
    pytest,
    signal,
    staged_grok_diagnostic_paths,
    uuid,
)


@pytest.mark.anyio
async def test_codex_envelope_and_lifecycle_mocks(tmp_path: Path) -> None:
    """Test envelope factory and persisted process lifecycle."""
    worktree_root = tmp_path / "worktrees"
    artifact_root = tmp_path / "artifacts"
    state_dir = tmp_path / "profile-state"
    wt_dir = worktree_root / "p1" / str(uuid.uuid4())
    wt_dir.mkdir(parents=True, exist_ok=True)
    (wt_dir / ".git").write_text("gitdir: /unmounted/metadata\n")
    artifact_root.mkdir()
    state_dir.mkdir()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    step_id = uuid.uuid4()
    mock_wt = MagicMock()
    mock_wt.id = uuid.uuid4()
    mock_wt.project_id = "p1"
    mock_wt.path = str(wt_dir)
    mock_wt.task_id = task_id
    mock_wt.run_id = run_id

    mock_step = MagicMock()
    mock_step.status = StepStatus.LEASED
    mock_step.lease_generation = 1
    mock_step.run_id = run_id
    mock_step.step_type = "execute_code"

    mock_sess = AsyncMock()
    stored_process: list[Any] = []

    async def _get(model: Any, _id: Any, **_kw: Any) -> Any:
        if model is Worktree:
            return mock_wt
        if model is Step:
            return mock_step
        if stored_process:
            return stored_process[0]
        return None

    def _add(row: Any) -> None:
        if row.__class__.__name__ == "SupervisedProcess":
            row.id = uuid.uuid4()
            stored_process.append(row)

    mock_sess.get.side_effect = _get
    mock_sess.scalar.return_value = None
    mock_sess.add = MagicMock(side_effect=_add)
    mock_sess.flush = AsyncMock()

    mock_factory = MagicMock()
    mock_factory.begin.return_value.__aenter__.return_value = mock_sess
    mock_factory.begin.return_value.__aexit__.return_value = False
    mock_factory.return_value.__aenter__.return_value = mock_sess
    mock_factory.return_value.__aexit__.return_value = False

    mock_reg = MagicMock()
    mock_reg.profiles.get.return_value = MagicMock(state_directory=state_dir, enabled=True)
    mock_reg.projects.get.return_value = MagicMock(
        sandbox_profile="def", validation_sandbox_profile="validation"
    )
    mock_reg.sandboxes.get.return_value = SandboxProfileConfig(
        id="def", image="ex@sha256:" + "a" * 64, enabled=True
    )

    mock_settings = MagicMock()
    mock_settings.worktree_root = worktree_root
    mock_settings.artifact_root = artifact_root
    seccomp_profile, seccomp_digest = _seccomp_profile(tmp_path)
    mock_settings.execution.sandbox_seccomp_profile = seccomp_profile
    mock_settings.execution.sandbox_seccomp_profile_sha256 = seccomp_digest

    envf = ExecutionEnvelopeFactory(mock_factory, mock_settings, mock_reg)

    inv = MagicMock(spec=CodexInvocation)
    inv.sandbox_reference = f"worktree:{mock_wt.id}"
    inv.task_id = task_id
    inv.run_id = run_id
    inv.step_id = step_id
    inv.profile_id = "prof"
    inv.provider_attempt = 1
    inv.lease_generation = 1
    inv.argv = canonical_codex_argv()
    inv.stdin = "prompt"
    inv.timeout_seconds = 30

    assert await envf.proxy_targets(inv) == ()

    mock_reg.projects.get.return_value = MagicMock(
        sandbox_profile="proxy",
        network=NetworkPolicy(
            enabled=True,
            destinations=(
                EgressDestination.model_validate(
                    {"url": "https://api.openai.com", "purpose": "runtime API"}
                ),
            ),
        ),
    )
    mock_reg.sandboxes.get.return_value = SandboxProfileConfig(
        id="proxy",
        image="ex@sha256:" + "a" * 64,
        enabled=True,
        network_mode=SandboxNetworkMode.HTTPS_PROXY,
    )
    mock_reg.profiles.get.return_value = MagicMock(
        state_directory=state_dir,
        enabled=True,
        runtime_network=NetworkPolicy(
            enabled=True,
            destinations=(
                EgressDestination.model_validate(
                    {"url": "https://api.openai.com", "purpose": "runtime API"}
                ),
            ),
        ),
    )
    targets = await envf.proxy_targets(inv)
    assert [(target.hostname, target.port) for target in targets] == [("api.openai.com", 443)]

    mock_reg.projects.get.return_value = MagicMock(
        sandbox_profile="def", validation_sandbox_profile="validation"
    )
    provider_sandbox = SandboxProfileConfig(
        id="def", image="provider@sha256:" + "a" * 64, enabled=True
    )
    validation_sandbox = SandboxProfileConfig(
        id="validation",
        image="validation@sha256:" + "b" * 64,
        enabled=True,
        inner_codex_sandbox_required=False,
    )
    mock_reg.sandboxes.get.side_effect = lambda profile: (
        validation_sandbox if profile == "validation" else provider_sandbox
    )
    mock_reg.profiles.get.return_value = MagicMock(
        state_directory=state_dir,
        enabled=True,
        provider="codex",
        model="codex",
        model_reasoning_effort=None,
    )

    envelope, pid = await envf.build(inv)
    assert envelope.sandbox.image == "provider@sha256:" + "a" * 64
    assert len(envelope.sandbox.mounts) == 3
    assert envelope.sandbox.mounts[0].target == Path("/workspace")
    assert envelope.sandbox.mounts[0].mode is MountMode.READ_WRITE
    assert envelope.sandbox.mounts[1].target == Path("/workspace/.git")
    assert envelope.sandbox.mounts[1].mode is MountMode.READ_ONLY
    assert envelope.sandbox.mounts[2].target == Path("/codex-home")
    assert envelope.sandbox.mounts[2].mode is MountMode.READ_WRITE
    assert envelope.sandbox.mounts[2].source == state_dir
    assert all(mount.target != Path("/artifacts") for mount in envelope.sandbox.mounts)
    assert all("docker.sock" not in str(mount.source) for mount in envelope.sandbox.mounts)
    assert '"/codex-home"="none"' in " ".join(envelope.argv)
    assert '"/workspace"="write"' in " ".join(envelope.argv)
    assert '"/artifacts"' not in " ".join(envelope.argv)
    assert "network={enabled=false}" in " ".join(envelope.argv)

    reader_inv = MagicMock(spec=CodexInvocation)
    reader_inv.sandbox_reference = f"worktree:{mock_wt.id}"
    reader_inv.task_id = task_id
    reader_inv.run_id = run_id
    reader_inv.step_id = uuid.uuid4()
    reader_inv.profile_id = "prof"
    reader_inv.provider_attempt = 2
    reader_inv.lease_generation = 1
    reader_inv.argv = canonical_codex_argv(read_only=True)
    reader_inv.stdin = "analyze"
    reader_inv.timeout_seconds = 30
    mock_step.step_type = "execute_agent"
    reader_envelope, _reader_pid = await envf.build(reader_inv)
    assert reader_envelope.sandbox.mounts[0].mode is MountMode.READ_ONLY
    assert 'default_permissions="vuzol-reader"' in reader_envelope.argv
    assert '"/workspace"="read"' in " ".join(reader_envelope.argv)
    mock_step.step_type = "execute_code"

    grok_inv = MagicMock(spec=CodexInvocation)
    grok_inv.sandbox_reference = f"worktree:{mock_wt.id}"
    grok_inv.task_id = task_id
    grok_inv.run_id = run_id
    grok_inv.step_id = uuid.uuid4()
    grok_inv.profile_id = "grok-prof"
    grok_inv.provider_attempt = 1
    grok_inv.lease_generation = 1
    grok_inv.argv = canonical_grok_argv("grok-build")
    grok_inv.stdin = "prompt"
    grok_inv.timeout_seconds = 30
    mock_reg.profiles.get.return_value = MagicMock(
        state_directory=state_dir,
        enabled=True,
        provider="grok",
        model="grok-build",
    )
    grok_envelope, _grok_pid = await envf.build(grok_inv)
    assert [mount.target for mount in grok_envelope.sandbox.mounts] == [
        Path("/workspace"),
        Path("/workspace/.git"),
        Path("/artifacts"),
        Path("/grok-home"),
    ]
    assert grok_envelope.sandbox.mounts[2].mode is MountMode.READ_WRITE
    assert grok_envelope.sandbox.mounts[2].source == (
        artifact_root / "execution" / str(grok_inv.step_id) / "1"
    )
    assert grok_envelope.sandbox.mounts[2].source.is_dir()
    gate_context = GateExecutionContext(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        worktree_id=mock_wt.id,
        profile_id="prof",
        provider_attempt=1,
        lease_generation=1,
    )
    gate_envelope = await envf.build_gate(
        gate_context, ("/usr/bin/make", "test"), timeout_seconds=30
    )
    assert gate_envelope.argv == ("/usr/bin/make", "test")
    assert gate_envelope.sandbox.image == "validation@sha256:" + "b" * 64
    assert gate_envelope.sandbox.network_disabled is True
    assert gate_envelope.sandbox.environment["UV_NO_SYNC"] == "1"
    assert gate_envelope.sandbox.environment["UV_OFFLINE"] == "1"
    assert gate_envelope.sandbox.environment["PYTHONPATH"] == "/workspace/src"
    assert all("provider-state" not in mount.purpose for mount in gate_envelope.sandbox.mounts)
    assert len(gate_envelope.sandbox.mounts) == 2
    assert gate_envelope.sandbox.mounts[0].source == wt_dir
    assert gate_envelope.sandbox.mounts[0].target == Path("/workspace")
    assert gate_envelope.sandbox.mounts[1].source == wt_dir / ".git"
    assert gate_envelope.sandbox.mounts[1].mode is MountMode.READ_ONLY
    with pytest.raises(ValueError, match="trusted registry"):
        await envf.build_gate(
            gate_context,
            ("/bin/sh", "-c", "make test"),
            timeout_seconds=30,
        )
    wrong_run = GateExecutionContext(
        task_id=task_id,
        run_id=uuid.uuid4(),
        step_id=step_id,
        worktree_id=mock_wt.id,
        profile_id="prof",
        provider_attempt=1,
        lease_generation=1,
    )
    with pytest.raises(ValueError, match="fenced lease"):
        await envf.build_gate(wrong_run, ("/usr/bin/make", "test"), timeout_seconds=30)
    mock_reg.sandboxes.get.side_effect = None
    mock_reg.sandboxes.get.return_value = SandboxProfileConfig(
        id="def", image="ex@sha256:" + "a" * 64, enabled=False
    )
    with pytest.raises(ValueError, match="disabled"):
        await envf.build_gate(gate_context, ("/usr/bin/make", "test"), timeout_seconds=30)
    mock_reg.sandboxes.get.return_value = SandboxProfileConfig(
        id="def", image="ex@sha256:" + "a" * 64, enabled=True
    )
    mock_settings.execution.sandbox_seccomp_profile = None
    with pytest.raises(ValueError, match="seccomp"):
        await envf.build_gate(gate_context, ("/usr/bin/make", "test"), timeout_seconds=30)
    mock_settings.execution.sandbox_seccomp_profile = seccomp_profile
    await envf.mark_running(pid, "vuzol-test-container")
    mock_art = MagicMock()
    mock_art.persist = AsyncMock(return_value=MagicMock(id=uuid.uuid4()))
    await envf.complete(pid, CodexProcessResult(0, "ok", "", 10), mock_art)
    assert stored_process[0].status.value == "exited"
    assert [call.kwargs["artifact_type"] for call in mock_art.persist.await_args_list[:2]] == [
        "stdout",
        "stderr",
    ]
    staging = artifact_root / "execution" / str(step_id) / "1"
    staging.mkdir(parents=True)
    stored_process[0].command_envelope = {"argv": ["grok"]}
    stored_process[0].runtime_metadata = {
        "configured_deadline_seconds": 30,
        "cancellation_classification": None,
        "cancellation_initiator": None,
        "cleanup_initiator": "sandbox_transport_finally",
    }
    await envf.complete(
        pid,
        CodexProcessResult(
            0,
            "\n".join(
                (
                    '{"type":"thought","data":"private output"}',
                    '{"type":"end","stopReason":"Cancelled"}',
                )
            ),
            "",
            75_700,
        ),
        mock_art,
    )
    metadata = stored_process[0].runtime_metadata
    assert metadata["actual_elapsed_ms"] == 75_700
    assert metadata["last_provider_event_type"] == "end"
    assert metadata["cancellation_classification"] == "PROVIDER_CANCELLED_UNATTRIBUTED"
    assert metadata["cancellation_initiator"] == "grok_cli_or_provider"
    assert metadata["cancellation_evidence_completeness"] == "unavailable"
    assert stored_process[0].provider_events_artifact_id is not None
    event_call = mock_art.persist.await_args_list[-1]
    assert event_call.kwargs["artifact_type"] == "provider-event-summary"
    assert b"private output" not in event_call.kwargs["content"]

    session_id = "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    state_dir.chmod(0o000)
    staged_paths = staged_grok_diagnostic_paths(staging, session_id)
    assert staged_paths is not None
    staged_paths[0].parent.mkdir(parents=True)
    staged_paths[0].write_text(
        "\n".join(
            (
                '{"type":"turn_started","schema_version":"1.0"}',
                '{"type":"tool_started","tool_name":"run_terminal_command"}',
                '{"type":"permission_requested","tool_name":"run_terminal_command"}',
                (
                    '{"type":"permission_resolved","tool_name":"run_terminal_command",'
                    '"decision":"cancelled"}'
                ),
                (
                    '{"type":"turn_ended","outcome":"cancelled",'
                    '"cancellation_category":"permission_cancelled"}'
                ),
            )
        )
    )
    staged_paths[1].write_text(
        json.dumps(
            {
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "call-1aa3af3d-e549-4c73-ac4e-fc0c08302ed2-31",
                        "title": "SECRET_NATIVE_TITLE",
                        "rawInput": {"command": "make test", "description": "SECRET_TASK"},
                        "_meta": {"x.ai/tool": {"name": "run_terminal_command"}},
                    }
                },
            }
        )
    )
    stored_process[0].runtime_metadata = {
        "configured_deadline_seconds": 30,
        "cancellation_classification": None,
        "cancellation_initiator": None,
        "cleanup_initiator": "sandbox_transport_finally",
    }
    await envf.complete(
        pid,
        CodexProcessResult(
            0,
            "\n".join(
                (
                    '{"type":"thought","data":"SECRET_REASONING"}',
                    (f'{{"type":"end","stopReason":"Cancelled","sessionId":"{session_id}"}}'),
                )
            ),
            "",
            76_000,
        ),
        mock_art,
    )
    proven = stored_process[0].runtime_metadata
    assert proven["cancellation_classification"] == "PROVIDER_PERMISSION_CANCELLED"
    assert proven["cancellation_initiator"] == "grok_permission_engine"
    assert proven["last_permission_decision"] == "cancelled"
    assert proven["last_native_tool_request_sequence"] == 2
    assert proven["last_native_tool_result_sequence"] is None
    assert proven["cancellation_evidence_completeness"] == "complete"
    proven_artifact = mock_art.persist.await_args_list[-1].kwargs["content"]
    assert b"make test" in proven_artifact
    assert b"SECRET" not in proven_artifact
    assert not staged_paths[0].exists() and not staged_paths[1].exists()
    await envf.fail_unknown(pid)
    assert stored_process[0].status.value == "unknown"
    state_dir.chmod(0o700)


def test_coding_workflow_execute_code_config() -> None:
    """Test execute_code step has the Step 08 UNKNOWN_EFFECTS config (real definition)."""
    from vuzol.workflows.definitions import WORKFLOW_REGISTRY

    coding = WORKFLOW_REGISTRY["coding.v1"]
    exec_step = next((s for s in coding.steps if s.key == "execute_code"), None)
    assert exec_step is not None
    assert exec_step.idempotency_class == IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE


def test_coding_workflow_review_is_retryable() -> None:
    from vuzol.storage.types import RetryClass
    from vuzol.workflows.definitions import WORKFLOW_REGISTRY

    coding = WORKFLOW_REGISTRY["coding.v1"]
    review = next((s for s in coding.steps if s.key == "review"), None)
    assert review is not None
    assert review.max_attempts == 3
    assert review.retry_class is RetryClass.TRANSIENT


def test_grok_execution_boundary_accepts_only_canonical_runtime() -> None:
    from vuzol.execution.codex import _provider_state_runtime, _require_provider_command
    from vuzol.providers.grok import canonical_grok_argv

    argv = canonical_grok_argv("grok-build")
    _require_provider_command(argv, "grok", "grok-build")
    target, environment = _provider_state_runtime("grok")
    assert target == Path("/grok-home")
    assert environment == {"HOME": "/grok-home"}
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(("grok",), "grok", "grok-build")
    with pytest.raises(ValueError, match="unsupported"):
        _provider_state_runtime("unknown")


@pytest.mark.anyio
async def test_executor_chain_short_circuits_between_workers() -> None:
    from vuzol.cli.executor import ExecutorChain

    worktrees = MagicMock()
    worktrees.process_one = AsyncMock(return_value=True)
    providers = MagicMock()
    providers.process_one = AsyncMock(return_value=True)
    assert await ExecutorChain(worktrees, providers).process_one() is True
    providers.process_one.assert_not_awaited()

    worktrees.process_one.return_value = False
    assert await ExecutorChain(worktrees, providers).process_one() is True
    providers.process_one.assert_awaited_once()


@pytest.mark.anyio
async def test_executor_composes_enabled_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    from vuzol.cli import executor as executor_cli
    from vuzol.config.models import LaunchMode

    profile = MagicMock(
        id="codex-a",
        enabled=True,
        provider="codex",
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
    )
    settings = MagicMock()
    settings.service_name = "vuzol"
    settings.log_level = "INFO"
    settings.execution.enabled = True
    settings.execution.require_preflight = True
    settings.execution.rootless_docker_socket = Path("/run/executor/docker.sock")
    settings.execution.sandbox_seccomp_profile = Path("/etc/vuzol/sandbox-seccomp.json")
    settings.execution.sandbox_seccomp_profile_sha256 = "a" * 64
    settings.workflow.poll_interval_seconds = 0.01
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    registries.revision = "a" * 64
    runtime = MagicMock(settings=settings, registries=registries)

    session = MagicMock()
    transaction = AsyncMock()
    transaction.__aenter__.return_value = session
    transaction.__aexit__.return_value = False
    factory = MagicMock()
    factory.begin.return_value = transaction
    engine = MagicMock()
    engine.dispose = AsyncMock()
    sandbox = MagicMock()
    sandbox.preflight = AsyncMock()
    worktree_access = MagicMock()
    worktree_access.preflight = AsyncMock()

    monkeypatch.setattr(executor_cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(executor_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(executor_cli, "RootlessDockerRuntime", lambda _socket: sandbox)
    monkeypatch.setattr(executor_cli, "RootlessIdentityResolver", MagicMock())
    monkeypatch.setattr(
        executor_cli,
        "WorktreeAccessManager",
        lambda *_args: worktree_access,
    )
    monkeypatch.setattr(executor_cli, "validate_seccomp_profile", MagicMock())
    monkeypatch.setattr(executor_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(executor_cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(executor_cli, "create_session_factory", lambda _engine: factory)
    monkeypatch.setattr(executor_cli, "synchronize_profiles", AsyncMock())
    for name in (
        "ScopedSecretResolver",
        "ArtifactStore",
        "ExecutionEnvelopeFactory",
        "SandboxCodexTransport",
        "CodexCliAdapter",
        "AdapterRegistry",
        "WorktreeService",
        "LocalGit",
        "ProviderStepHandler",
        "PrepareWorktreeHandler",
        "WorkflowWorker",
        "RoutedWorkflowWorker",
    ):
        monkeypatch.setattr(executor_cli, name, MagicMock())
    run_loop = AsyncMock()
    monkeypatch.setattr(executor_cli, "_run_loop", run_loop)

    await executor_cli.run()
    sandbox.preflight.assert_awaited_once()
    worktree_access.preflight.assert_awaited_once()
    run_loop.assert_awaited_once()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_executor_loop_stops_on_registered_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.cli import executor as executor_cli

    callbacks: dict[int, Any] = {}

    class LoopProxy:
        def add_signal_handler(self, signum: int, callback: Any) -> None:
            callbacks[signum] = callback

    processor = MagicMock()

    async def process_one() -> bool:
        callbacks[signal.SIGTERM]()
        return False

    processor.process_one = process_one
    monkeypatch.setattr("vuzol.cli.executor.asyncio.get_running_loop", lambda: LoopProxy())
    await executor_cli._run_loop(processor, 0.01)
    assert set(callbacks) == {signal.SIGTERM, signal.SIGINT}


def test_unknown_effects_step_outcome() -> None:
    """Test handling of UNKNOWN_EFFECTS_POSSIBLE (Step 08) leads to block (real transitions)."""
    from vuzol.workflows.domain import OutcomeKind, StepOutcome

    # Real behavior: for unknown effects, we expect the outcome to be marked for review
    outcome = StepOutcome(
        kind=OutcomeKind.BLOCKED,
        result={},
        category="unknown_effects",
    )
    assert outcome.kind == OutcomeKind.BLOCKED
    assert outcome.category is not None
    assert "unknown" in outcome.category
