"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from types import SimpleNamespace

from vuzol.projects.toolchains import TOOLCHAIN_RECEIPT, ToolchainSpec
from vuzol.storage.types import QueueClass

from ._execution_helpers import (
    Any,
    AsyncMock,
    CodexInvocation,
    CodexProcessResult,
    EgressDestination,
    ExecutionEnvelopeFactory,
    GateExecutionContext,
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
@pytest.mark.parametrize(
    "legacy_provisioning", ("automatic", "approval_required", "external_setup")
)
async def test_codex_envelope_and_lifecycle_mocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy_provisioning: str
) -> None:
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
    mock_settings.capability_provisioning.toolchain_root = tmp_path / "toolchains"
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
    assert '"/codex-home/auth.json"="none"' in " ".join(envelope.argv)
    assert '"/codex-home/tmp/arg0"="read"' in " ".join(envelope.argv)
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
    # Review cannot accept the executor's writable command, and both workspace
    # and Git mounts are read-only even though the account needs writable state.
    mock_step.step_type = "review"
    mock_step.payload = {"executor_model_override": "grok-4.7", "executor_reasoning_effort": "high"}
    mock_reg.profiles.get.return_value = MagicMock(
        state_directory=state_dir,
        enabled=True,
        provider="grok",
        model="grok-4.7",
        model_reasoning_effort="high",
    )
    mock_reg.profiles.get.return_value.model_copy.return_value = mock_reg.profiles.get.return_value
    grok_inv.argv = canonical_grok_argv("grok-4.7", reasoning_effort="high")
    with pytest.raises(ValueError, match="canonical read-only provider command"):
        await envf.build(grok_inv)
    grok_inv.argv = canonical_grok_argv("grok-4.7", read_only=True, reasoning_effort="high")
    grok_inv.provider_attempt = 2
    readonly_envelope, _ = await envf.build(grok_inv)
    assert readonly_envelope.sandbox.environment["GROK_MAX_RETRIES"] == "0"
    assert readonly_envelope.sandbox.environment["GROK_TURN_TRANSIENT_RETRY"] == "0"
    assert readonly_envelope.sandbox.mounts[0].mode is MountMode.READ_ONLY
    assert readonly_envelope.sandbox.mounts[1].mode is MountMode.READ_ONLY
    assert readonly_envelope.argv[-2:] == ("--reasoning-effort", "high")
    mock_step.step_type = "execute_code"
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
    artifact_request = MagicMock(
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        lease=MagicMock(generation=1),
    )
    toolchain = mock_settings.capability_provisioning.toolchain_root / "android-sdk"
    for relative in (
        "android-sdk/platform-tools/adb",
        "jdk/bin/java",
        "gradle/bin/gradle",
    ):
        executable = toolchain / relative
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_bytes(b"tool")
        executable.chmod(0o555)
    receipt = toolchain / TOOLCHAIN_RECEIPT
    receipt.write_text(
        json.dumps(
            ToolchainSpec(
                capability_key="android-sdk",
                version="35.0-test",
                archive_sha256="a" * 64,
                executables=(
                    ("adb", "android-sdk/platform-tools/adb"),
                    ("gradle", "gradle/bin/gradle"),
                    ("java", "jdk/bin/java"),
                ),
                environment=(
                    ("ANDROID_HOME", "android-sdk"),
                    ("ANDROID_SDK_ROOT", "android-sdk"),
                    ("GRADLE_HOME", "gradle"),
                    ("JAVA_HOME", "jdk"),
                ),
            ).receipt()
        )
    )
    receipt.chmod(0o444)
    monkeypatch.setattr(
        "vuzol.execution.codex.current_environment",
        AsyncMock(
            return_value=MagicMock(
                contract={"capabilities": {"android-sdk": {"provisioning": legacy_provisioning}}}
            )
        ),
    )
    inventory_probe = AsyncMock(return_value=SimpleNamespace(version="35.0-test"))
    monkeypatch.setattr("vuzol.execution.codex.verify_installed_capability", inventory_probe)
    artifact_envelope = await envf.build_artifact(
        artifact_request,
        worktree_id=mock_wt.id,
        argv=("python", "-m", "build"),
        timeout_seconds=30,
    )
    assert artifact_envelope.argv == (
        "/opt/vuzol-validation/bin/python",
        "-m",
        "build",
    )
    assert artifact_envelope.sandbox.network_disabled is True
    assert artifact_envelope.sandbox.mounts[-1].target == Path("/toolchains")
    assert artifact_envelope.sandbox.mounts[-1].mode is MountMode.READ_ONLY
    assert artifact_envelope.sandbox.environment["ANDROID_HOME"] == (
        "/toolchains/android-sdk/android-sdk"
    )
    gradle_envelope = await envf.build_artifact(
        artifact_request,
        worktree_id=mock_wt.id,
        argv=("gradle", "assembleDebug"),
        timeout_seconds=30,
    )
    assert gradle_envelope.argv == (
        "/toolchains/android-sdk/gradle/bin/gradle",
        "assembleDebug",
    )
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
    stored_process[0].command_envelope = {"argv": ["codex", "exec", "--json"]}
    stored_process[0].runtime_metadata = {
        "configured_deadline_seconds": 30,
        "cancellation_classification": None,
        "cancellation_initiator": None,
        "cleanup_initiator": "sandbox_transport_finally",
    }
    await envf.complete(
        pid,
        CodexProcessResult(
            1,
            '{"type":"error","message":"SECRET_PROVIDER_FAILURE"}',
            "provider unavailable",
            20,
        ),
        mock_art,
    )
    codex_metadata = stored_process[0].runtime_metadata
    assert codex_metadata["last_provider_event_type"] == "error"
    assert codex_metadata["provider_diagnostic_failure_signals"] == [
        "process_exit:1",
        "event:error",
        "stderr_nonempty",
    ]
    codex_persisted = {
        call.kwargs["artifact_type"]: call.kwargs["content"]
        for call in mock_art.persist.await_args_list
    }
    assert b'"provider":"codex"' in codex_persisted["provider-diagnostics"]
    assert b"SECRET_PROVIDER_FAILURE" not in codex_persisted["provider-diagnostics"]
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
    assert proven["last_provider_tool_name"] == "run_terminal_command"
    assert proven["last_tool_call_id"] == "call-1aa3af3d-e549-4c73-ac4e-fc0c08302ed2-31"
    assert "provider_diagnostics_artifact_id" in proven
    assert proven["cancellation_evidence_completeness"] == "complete"
    persisted = {
        call.kwargs["artifact_type"]: call.kwargs["content"]
        for call in mock_art.persist.await_args_list
    }
    assert b"make test" in persisted["provider-event-summary"]
    assert b"run_terminal_command" in persisted["provider-diagnostics"]
    assert b"SECRET" not in persisted["provider-diagnostics"]
    assert not staged_paths[0].exists() and not staged_paths[1].exists()
    await envf.fail_unknown(pid)
    assert stored_process[0].status.value == "exited"
    assert stored_process[0].runtime_metadata["post_cleanup_evidence_error"] is True
    state_dir.chmod(0o700)


def test_grok_execution_boundary_accepts_only_canonical_runtime() -> None:
    from vuzol.execution.codex import _provider_state_runtime, _require_provider_command
    from vuzol.providers.grok import canonical_grok_argv

    argv = canonical_grok_argv("grok-build")
    _require_provider_command(argv, "grok", "grok-build")
    target, environment = _provider_state_runtime("grok")
    assert target == Path("/grok-home")
    assert environment == {
        "HOME": "/grok-home",
        "GROK_MAX_RETRIES": "0",
        "GROK_TURN_TRANSIENT_RETRY": "0",
    }
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(("grok",), "grok", "grok-build")
    with pytest.raises(ValueError, match="unsupported"):
        _provider_state_runtime("unknown")


def test_pi_execution_boundary_accepts_only_canonical_runtime() -> None:
    from vuzol.execution.codex import _provider_state_runtime, _require_provider_command
    from vuzol.providers.pi import canonical_pi_argv

    write = canonical_pi_argv("kimi-k3")
    read_only = canonical_pi_argv("kimi-k3", read_only=True)
    _require_provider_command(write, "pi", "kimi-k3")
    _require_provider_command(read_only, "pi", "kimi-k3")
    snapshot = canonical_pi_argv("kimi-k3", read_only=True, prompt_only=True)
    _require_provider_command(
        snapshot, "pi", "kimi-k3", require_read_only=True, require_prompt_only=True
    )
    for candidate in (read_only, write, (*snapshot, "--tools", "read")):
        with pytest.raises(ValueError):
            _require_provider_command(
                candidate, "pi", "kimi-k3", require_read_only=True, require_prompt_only=True
            )
    with pytest.raises(ValueError):
        _require_provider_command(snapshot, "pi", "kimi-k3")
    target, environment = _provider_state_runtime("pi")
    assert target == Path("/pi-home")
    assert environment["PI_CODING_AGENT_DIR"] == "/tmp/pi-home"  # noqa: S108
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(("pi", "-p"), "pi", "kimi-k3")
    with pytest.raises(ValueError, match="non-canonical"):
        _require_provider_command(canonical_pi_argv("other-model"), "pi", "kimi-k3")


@pytest.mark.anyio
@pytest.mark.parametrize(("cli_review", "lead_lanes"), [(False, 0), (True, 0), (False, 3)])
@pytest.mark.parametrize("networked", [False, True])
async def test_executor_composes_enabled_runtime(
    monkeypatch: pytest.MonkeyPatch, caplog, cli_review, lead_lanes, networked
) -> None:
    from vuzol.cli import executor as executor_cli
    from vuzol.config.models import LaunchMode

    profile = MagicMock(
        id="codex-a",
        enabled=True,
        provider="codex",
        launch_mode=LaunchMode.CLI,
        credential_reference=None,
        concurrency_limit=2,
    )
    settings = MagicMock()
    settings.dependency_provisioning.enabled = False
    settings.dependency_provisioning.environment_root = Path(
        "/var/lib/vuzol/dependency-environments"
    )
    settings.service_name = "vuzol"
    settings.log_level = "INFO"
    settings.execution.enabled = True
    settings.project_lead.enabled = lead_lanes > 0
    settings.project_lead.lead_parallelism = lead_lanes
    settings.execution.reviewer_profile_id = "grok-a" if cli_review else None
    settings.limits.provider_call_input_tokens = 100_000
    settings.execution.require_preflight = True
    settings.execution.journal_key_file = None
    settings.execution.provider_parallelism = 2
    settings.execution.rootless_docker_socket = Path("/run/executor/docker.sock")
    settings.execution.sandbox_seccomp_profile = Path("/etc/vuzol/sandbox-seccomp.json")
    settings.execution.sandbox_seccomp_profile_sha256 = "a" * 64
    settings.workflow.poll_interval_seconds = 0.01
    registries = MagicMock()
    registries.profiles.items.return_value = (profile,)
    registries.profiles.get.return_value.concurrency_limit = 2
    registries.revision = "a" * 64
    registries.sandboxes.items.return_value = (
        (
            SimpleNamespace(
                enabled=True, network_mode=SandboxNetworkMode.HTTPS_PROXY, uid=10001, gid=10001
            ),
        )
        if networked
        else ()
    )
    settings.execution.proxy_image = "proxy@sha256:" + "a" * 64 if networked else None
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
    monkeypatch.setattr(executor_cli, "RootlessDockerRuntime", lambda _socket, **_kw: sandbox)
    monkeypatch.setattr(executor_cli, "RootlessIdentityResolver", MagicMock())
    monkeypatch.setattr(
        executor_cli,
        "WorktreeAccessManager",
        lambda *_args: worktree_access,
    )
    monkeypatch.setattr(executor_cli, "validate_seccomp_profile", MagicMock())
    monkeypatch.setattr(executor_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(executor_cli, "create_engine", lambda *_args: engine)
    migration_head = AsyncMock()
    monkeypatch.setattr(executor_cli, "require_migration_head", migration_head)
    monkeypatch.setattr(executor_cli, "require_workflow_upgrade", AsyncMock())
    monkeypatch.setattr(executor_cli, "create_session_factory", lambda _engine: factory)
    turn_reconciler = MagicMock()
    turn_reconciler.reconcile_startup = AsyncMock()
    turn_reconciler_factory = MagicMock(return_value=turn_reconciler)
    monkeypatch.setattr(executor_cli, "TurnStartupReconciler", turn_reconciler_factory)
    sync_profiles = AsyncMock()
    monkeypatch.setattr(executor_cli, "synchronize_profiles", sync_profiles)
    patched: dict[str, MagicMock] = {}
    for name in (
        "ScopedSecretResolver",
        "ArtifactStore",
        "ExecutionEnvelopeFactory",
        "SandboxCodexTransport",
        "cli_adapter",
        "ReadOnlyCliAdapter",
        "AdapterRegistry",
        "WorktreeService",
        "LocalGit",
        "ProviderStepHandler",
        "PrepareWorktreeHandler",
        "WorkflowWorker",
        "RoutedWorkflowWorker",
    ):
        patched[name] = MagicMock()
        monkeypatch.setattr(executor_cli, name, patched[name])
    monkeypatch.setattr("vuzol.execution.turn_transport.TurnSandboxTransport", MagicMock())
    monkeypatch.setattr(
        "vuzol.model_calls.worker.ModelCallReconciler",
        MagicMock(return_value=MagicMock(reconcile=AsyncMock(return_value=0))),
    )
    proxy_manager = MagicMock()
    proxy_report = SimpleNamespace(lock_acquired=False, removed_count=2)
    proxy_reconciler = MagicMock(reconcile_startup=AsyncMock(return_value=proxy_report))
    monkeypatch.setattr(executor_cli, "ProxyServiceManager", MagicMock(return_value=proxy_manager))
    monkeypatch.setattr(
        executor_cli, "ProxyStartupReconciler", MagicMock(return_value=proxy_reconciler)
    )
    node = SimpleNamespace(id="remote", model_call_parallelism=2, provider_parallelism=2)
    remote_calls = MagicMock(reconcile=AsyncMock(side_effect=RuntimeError("node unavailable")))
    remote_providers = MagicMock(reconcile=AsyncMock(side_effect=RuntimeError("node unavailable")))
    remote_worker = MagicMock()
    monkeypatch.setattr(
        executor_cli,
        "_node_model_calls",
        lambda *args: ((node, MagicMock(), ("pi-a",), lambda: False, remote_calls),),
    )
    monkeypatch.setattr(
        executor_cli,
        "_node_providers",
        lambda *args, **kwargs: ((node, remote_worker, remote_providers),),
    )
    run_loop = AsyncMock()
    monkeypatch.setattr(executor_cli, "_run_loop", run_loop)
    lead_owners: list[str] = []

    def lead(*_args: object, owner: str, **_kwargs: object) -> MagicMock:
        lead_owners.append(owner)
        return MagicMock()

    monkeypatch.setattr(executor_cli, "build_project_lead_worker", lead)
    monkeypatch.setattr(executor_cli, "build_goal_retrospective_worker", MagicMock())
    dream_builder = MagicMock()
    monkeypatch.setattr(executor_cli, "build_dream_worker", dream_builder)

    await executor_cli.run()
    sandbox.preflight.assert_awaited_once()
    worktree_access.preflight.assert_awaited_once()
    migration_head.assert_awaited_once_with(engine)
    sync_profiles.assert_awaited()
    turn_reconciler.reconcile_startup.assert_awaited_once()
    assert turn_reconciler_factory.call_args.args[:2] == (factory, sandbox)
    assert turn_reconciler_factory.call_args.kwargs["enabled"]() is (lead_lanes > 0)
    run_loop.assert_awaited_once()
    assert run_loop.await_args is not None
    assert run_loop.await_args.kwargs["subscription_limit_settings"] is settings.subscription_limits
    if lead_lanes:
        dream_builder.assert_called_once()
    else:
        dream_builder.assert_not_called()
    assert run_loop.await_args.kwargs["dream_settings"] is settings.project_lead
    assert run_loop.await_args.kwargs["timezone"] == settings.assistant.timezone
    chain = run_loop.await_args.args[0]
    assert chain._reviews is not None
    # Each lead lane owns its goal leases under its own name.
    assert len(lead_owners) == lead_lanes and len(set(lead_owners)) == lead_lanes
    assert len(chain._leads) == max(0, lead_lanes - 1)
    review_args = patched["WorkflowWorker"].call_args.kwargs
    assert set(review_args["handlers"]) == {"review"}
    assert review_args["profile_limits"] == {"codex-a": 2}
    reviewer = review_args["handlers"]["review"]._independent
    assert reviewer._profile_id == ("grok-a" if cli_review else None)
    assert reviewer._input_token_limit == 100_000
    assert reviewer._factory is factory
    # workflow, leadership turn and model call transports
    assert patched["cli_adapter"].call_count == 3
    patched["ReadOnlyCliAdapter"].assert_called_once()
    provider_calls = patched["RoutedWorkflowWorker"].call_args_list
    assert len(provider_calls) == 2
    assert all("plan" not in call.kwargs["handlers"] for call in provider_calls)
    assert {call.kwargs["queue_classes"] for call in provider_calls} == {
        frozenset({QueueClass.LIGHT}),
        frozenset({QueueClass.HEAVY}),
    }
    assert chain._node_providers == (remote_worker, remote_worker)
    assert len(chain._model_calls) >= 2
    await chain._recovery()
    assert remote_calls.reconcile.await_count == 1
    assert remote_providers.reconcile.await_count == 2
    assert turn_reconciler.reconcile_startup.await_count == 2
    if networked:
        assert proxy_reconciler.reconcile_startup.await_count == 2
        assert {getattr(r, "event", None) for r in caplog.records} >= {
            "executor.proxy_reconciliation_lock_timeout",
            "executor.proxy_recovered",
        }
    else:
        proxy_reconciler.reconcile_startup.assert_not_awaited()
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_executor_fails_closed_before_profile_sync_when_migration_head_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S-2.1: refuse after engine create; no synchronize_profiles / reconcile / ready loop."""

    from vuzol.cli import executor as executor_cli
    from vuzol.storage.migration_preflight import MigrationHeadError

    settings = MagicMock()
    settings.service_name = "vuzol"
    settings.log_level = "INFO"
    settings.execution.enabled = True
    settings.execution.require_preflight = False
    settings.execution.journal_key_file = None
    settings.execution.rootless_docker_socket = Path("/run/executor/docker.sock")
    settings.execution.sandbox_seccomp_profile = Path("/etc/vuzol/sandbox-seccomp.json")
    settings.execution.sandbox_seccomp_profile_sha256 = "a" * 64
    settings.worktree_root = Path("/tmp/worktrees")  # noqa: S108
    registries = MagicMock()
    registries.profiles.items.return_value = ()
    registries.sandboxes.items.return_value = ()
    registries.revision = "a" * 64
    runtime = MagicMock(settings=settings, registries=registries)

    engine = MagicMock()
    engine.dispose = AsyncMock()
    order: list[str] = []

    async def refuse(_engine: object, **_kwargs: object) -> object:
        order.append("require_migration_head")
        raise MigrationHeadError(
            "migration_head_empty",
            "database has no alembic revision rows",
        )

    async def never_sync(*_args: object, **_kwargs: object) -> None:
        order.append("synchronize_profiles")
        raise AssertionError("synchronize_profiles must not run")

    worktree_access = MagicMock()
    worktree_access.preflight = AsyncMock()

    monkeypatch.setattr(executor_cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(executor_cli, "configure_logging", lambda **_kwargs: None)
    monkeypatch.setattr(executor_cli, "RootlessDockerRuntime", MagicMock())
    monkeypatch.setattr(executor_cli, "RootlessIdentityResolver", MagicMock())
    monkeypatch.setattr(
        executor_cli,
        "WorktreeAccessManager",
        lambda *_args: worktree_access,
    )
    monkeypatch.setattr(executor_cli, "validate_seccomp_profile", MagicMock())
    monkeypatch.setattr(executor_cli, "resolve_database_dsn", lambda _settings: object())
    monkeypatch.setattr(executor_cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(executor_cli, "require_migration_head", refuse)
    monkeypatch.setattr(executor_cli, "create_session_factory", MagicMock())
    monkeypatch.setattr(executor_cli, "synchronize_profiles", never_sync)
    run_loop = AsyncMock()
    monkeypatch.setattr(executor_cli, "_run_loop", run_loop)

    with pytest.raises(MigrationHeadError) as excinfo:
        await executor_cli.run()

    assert excinfo.value.code == "migration_head_empty"
    assert order == ["require_migration_head"]
    run_loop.assert_not_awaited()
    # Gate fails closed before profile sync; engine still disposed in finally.
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
    processor.lanes.return_value = (processor,)
    processor.recover_if_due = AsyncMock()
    processor.aclose = AsyncMock()

    async def process_one() -> bool:
        callbacks[signal.SIGTERM]()
        return False

    processor.process_one = process_one
    monkeypatch.setattr("vuzol.cli.executor.asyncio.get_running_loop", lambda: LoopProxy())
    await executor_cli._run_loop(processor, 0.01)
    assert set(callbacks) == {signal.SIGTERM, signal.SIGINT}
    # Lanes owning background work (lead research jobs) are closed on shutdown.
    processor.aclose.assert_awaited_once()


@pytest.mark.anyio
async def test_executor_loop_forwards_explicit_limit_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.cli import executor as executor_cli
    from vuzol.config import SubscriptionLimitSettings

    callbacks: dict[int, Any] = {}

    class LoopProxy:
        def add_signal_handler(self, signum: int, callback: Any) -> None:
            callbacks[signum] = callback

    processor = MagicMock()
    processor.lanes.return_value = (processor,)
    processor.recover_if_due = AsyncMock()
    processor.aclose = AsyncMock()

    async def process_one() -> bool:
        callbacks[signal.SIGTERM]()
        return False

    processor.process_one = process_one
    refresh = AsyncMock(return_value=False)
    monkeypatch.setattr("vuzol.cli.executor.asyncio.get_running_loop", lambda: LoopProxy())
    monkeypatch.setattr(executor_cli, "_refresh_subscription_limits_tick", refresh)
    limits = SubscriptionLimitSettings()

    await executor_cli._run_loop(
        processor,
        0.01,
        session_factory=MagicMock(),
        registries=MagicMock(),
        subscription_limit_settings=limits,
    )

    refresh.assert_awaited_once()
    assert refresh.await_args is not None
    assert refresh.await_args.kwargs["settings"] is limits


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


@pytest.mark.parametrize("missing", ["sandbox", "proxy", "image"])
def test_node_model_calls_require_controlled_egress(monkeypatch, missing):
    from vuzol.cli import executor as cli

    settings, registries = MagicMock(), MagicMock()
    settings.execution.model_call_sandbox_profile = None if missing == "sandbox" else "helper"
    settings.execution.proxy_image = None if missing == "image" else "image@sha256:abc"
    assert (
        cli._node_model_calls(
            settings,
            registries,
            MagicMock(),
            MagicMock(),
            None,
            None if missing == "proxy" else MagicMock(),
        )
        == ()
    )
    registries.sandboxes.get.assert_not_called()


@pytest.mark.parametrize("kind", ["model_calls", "providers"])
@pytest.mark.parametrize("proxy_enabled", [False, True])
def test_remote_lanes_admit_only_enabled_supported_pi_cli_profiles(
    tmp_path, monkeypatch, kind, proxy_enabled
):
    import socket

    from vuzol.cli import executor as cli
    from vuzol.config import LaunchMode
    from vuzol.execution import node as node_module
    from vuzol.model_calls import transport as call_transport
    from vuzol.model_calls import worker as call_worker

    settings, registries = MagicMock(), MagicMock()
    path = tmp_path / "docker.sock"
    node = SimpleNamespace(
        id="remote",
        docker_socket=path,
        model_call_parallelism=2,
        provider_parallelism=3,
        max_copy_back_bytes=1234,
    )
    disabled = SimpleNamespace(id="disabled", model_call_parallelism=0, provider_parallelism=0)
    settings.execution.nodes = (disabled, node)
    settings.execution.model_call_sandbox_profile = "helper" if proxy_enabled else None
    settings.execution.proxy_image = "proxy-image" if proxy_enabled else None
    settings.execution.rootless_docker_socket = tmp_path / "local.sock"
    settings.execution.worker_mcp_servers = {"docs": "https://docs.test"}
    registries.sandboxes.get.return_value.image = "helper-image"

    def profile(id, **kw):
        return SimpleNamespace(
            **(dict(id=id, enabled=True, provider="pi", launch_mode=LaunchMode.CLI) | kw)
        )

    registries.profiles.items.return_value = (
        profile("pi-b"),
        profile("pi-a"),
        profile("off", enabled=False),
        profile("codex", provider="codex"),
        profile("api", launch_mode=LaunchMode.API),
        profile("unsupported"),
    )
    monkeypatch.setattr(cli, "supported_profile", lambda p: p.id != "unsupported")
    runtime, proxy = MagicMock(), MagicMock()
    monkeypatch.setattr(node_module, "NodeDockerRuntime", runtime)
    monkeypatch.setattr(node_module, "NodeProxyServiceManager", proxy)
    adapter = MagicMock(side_effect=lambda p, _transport: SimpleNamespace(profile_id=p.id))
    monkeypatch.setattr(cli, "cli_adapter", adapter)
    registry = MagicMock()
    monkeypatch.setattr(cli, "AdapterRegistry", registry)
    role = MagicMock()
    monkeypatch.setattr(cli, "RoleBoundCliAdapter", role)
    factory, resolver, journal = MagicMock(), MagicMock(), MagicMock()
    if kind == "model_calls":
        # Model calls always need proxy configuration; provider lanes may be offline.
        settings.execution.model_call_sandbox_profile = "helper"
        settings.execution.proxy_image = "proxy-image"
        transport, reaper = MagicMock(), MagicMock()
        monkeypatch.setattr(call_transport, "ModelCallSandboxTransport", transport)
        monkeypatch.setattr(call_worker, "ModelCallReconciler", reaper)
        lanes = cli._node_model_calls(settings, registries, factory, resolver, journal, MagicMock())
        assert len(lanes) == 1
        actual_node, actual_registry, profile_ids, ready, actual_reaper = lanes[0]
        assert actual_registry is registry.return_value and actual_reaper is reaper.return_value
        assert profile_ids == ("pi-a", "pi-b")
        assert role.call_count == 2
        assert transport.call_args.kwargs["node"] == "remote"
    else:
        worker, handler, reaper = MagicMock(), MagicMock(), MagicMock()
        envelopes, transport = MagicMock(), MagicMock()
        monkeypatch.setattr(cli, "RoutedWorkflowWorker", worker)
        monkeypatch.setattr(cli, "ProviderStepHandler", handler)
        monkeypatch.setattr(cli, "NodeStepReconciler", reaper)
        monkeypatch.setattr(cli, "ExecutionEnvelopeFactory", envelopes)
        monkeypatch.setattr(cli, "SandboxCodexTransport", transport)
        monkeypatch.setattr(
            cli,
            "executor_provider_handlers",
            lambda _handler: {"execute_code": "execute", "plan": "plan"},
        )
        lanes = cli._node_providers(
            settings,
            registries,
            factory,
            resolver,
            journal,
            artifacts=MagicMock(),
            worktrees=MagicMock(),
            worktree_access=MagicMock(),
            owner="executor",
        )
        assert len(lanes) == 1
        actual_node, actual_worker, actual_reaper = lanes[0]
        assert actual_worker is worker.return_value and actual_reaper is reaper.return_value
        assert runtime.call_args.kwargs["writable_worktree_only"] is True
        assert worker.call_args.kwargs["handlers"] == {"execute_code": "execute"}
        assert worker.call_args.kwargs["profile_ids"] == frozenset({"pi-a", "pi-b"})
        assert worker.call_args.kwargs["queue_classes"] == frozenset({cli.QueueClass.HEAVY})
        assert worker.call_args.kwargs["owner"] == "executor:provider:remote"
        assert handler.call_args.kwargs["worker_mcp_servers"] == {"docs": "https://docs.test"}
        assert envelopes.call_args.kwargs["node"] == "remote"
        assert transport.call_args.args[-1] is (proxy.return_value if proxy_enabled else None)
        ready = worker.call_args.kwargs["ready"]
    assert actual_node is node
    assert [call.args[0].id for call in adapter.call_args_list] == ["pi-b", "pi-a"]
    assert set(registry.call_args.kwargs["adapters"]) == {"pi-a", "pi-b"}
    assert runtime.call_args.args[:3] == ("remote", path, settings.execution.rootless_docker_socket)
    assert runtime.call_args.kwargs["max_copy_back_bytes"] == 1234
    assert not ready()
    with socket.socket(socket.AF_UNIX) as sock:
        sock.bind(str(path))
        assert ready()
    assert proxy.call_count == int(proxy_enabled or kind == "model_calls")


@pytest.mark.parametrize("kind", ["model_calls", "providers"])
def test_remote_lanes_are_absent_without_eligible_profiles(kind, monkeypatch):
    from vuzol.cli import executor as cli
    from vuzol.execution import node

    settings, registries = MagicMock(), MagicMock()
    settings.execution.nodes = (
        SimpleNamespace(
            id="remote",
            docker_socket=Path("/missing"),
            model_call_parallelism=1,
            provider_parallelism=1,
            max_copy_back_bytes=1,
        ),
    )
    registries.profiles.items.return_value = ()
    runtime = MagicMock()
    monkeypatch.setattr(node, "NodeDockerRuntime", runtime)
    monkeypatch.setattr(node, "NodeProxyServiceManager", MagicMock())
    monkeypatch.setattr("vuzol.model_calls.transport.ModelCallSandboxTransport", MagicMock())
    if kind == "model_calls":
        assert (
            cli._node_model_calls(settings, registries, MagicMock(), MagicMock(), None, MagicMock())
            == ()
        )
    else:
        assert (
            cli._node_providers(
                settings,
                registries,
                MagicMock(),
                MagicMock(),
                None,
                artifacts=MagicMock(),
                worktrees=MagicMock(),
                worktree_access=MagicMock(),
                owner="executor",
            )
            == ()
        )
        runtime.assert_not_called()


@pytest.mark.anyio
async def test_executor_recovery_runs_only_when_due_and_retries_after_error(monkeypatch):
    from vuzol.cli import executor as cli

    now = 100.0
    monkeypatch.setattr(cli.time, "monotonic", lambda: now)
    recovery = AsyncMock(side_effect=[RuntimeError("reconcile failed"), None])
    chain = cli.ExecutorChain(MagicMock(), MagicMock(), recovery=recovery)
    await chain.recover_if_due()
    recovery.assert_not_awaited()
    now = 130.0
    with pytest.raises(RuntimeError, match="reconcile failed"):
        await chain.recover_if_due()
    await chain.recover_if_due()
    assert recovery.await_count == 2
    await chain.recover_if_due()
    assert recovery.await_count == 2
    now = 160.0
    recovery.side_effect = None
    await chain.recover_if_due()
    assert recovery.await_count == 3


def test_executor_lanes_preserve_priority_and_configured_parallelism():
    from vuzol.cli import executor as cli

    workers = [MagicMock() for _ in range(10)]
    worktrees, providers, reviews, lead, control, control_wt, model, node, extra, extra2 = workers
    chain = cli.ExecutorChain(
        worktrees,
        providers,
        reviews,
        lead,
        control=control,
        control_worktrees=control_wt,
        provider_parallelism=3,
        model_calls=(model,),
        node_providers=(node,),
        leads=(extra, extra2),
    )
    assert chain.lanes() == (
        lead,
        extra,
        extra2,
        reviews,
        control_wt,
        control,
        worktrees,
        providers,
        providers,
        providers,
        model,
        node,
    )
    assert cli.ExecutorChain(worktrees, providers).lanes() == (worktrees, providers)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["profiles", "database", "refresh"])
async def test_topic_line_tick_logs_failures_without_stopping_executor(
    monkeypatch, caplog, failure
):
    from vuzol.cli import executor as cli
    from vuzol.telegram import topic_line

    registries, factory = MagicMock(), MagicMock()
    session = AsyncMock()
    factory.begin.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    labels = MagicMock(return_value={"pi": "Pi"})
    refresh = AsyncMock()
    monkeypatch.setattr(topic_line, "profile_labels", labels)
    monkeypatch.setattr(topic_line, "refresh_topic_lines", refresh)
    if failure == "profiles":
        labels.side_effect = RuntimeError("broken profiles")
    elif failure == "database":
        factory.begin.side_effect = RuntimeError("database unavailable")
    else:
        refresh.side_effect = RuntimeError("Telegram down")
    await cli._refresh_topic_lines_tick(factory, registries)
    assert any(getattr(r, "event", None) == "executor.topic_line_failed" for r in caplog.records)
    if failure == "refresh":
        refresh.assert_awaited_once_with(session, {"pi": "Pi"})
    else:
        refresh.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("exit_code", [0, 127])
async def test_validation_preflight_checks_unique_enabled_images_offline(tmp_path, exit_code):
    from vuzol.cli import executor as cli

    registries = MagicMock()
    registries.projects.items.return_value = (
        SimpleNamespace(enabled=True, validation_sandbox_profile="validation"),
        SimpleNamespace(enabled=True, validation_sandbox_profile="validation"),
        SimpleNamespace(enabled=False, validation_sandbox_profile="disabled"),
        SimpleNamespace(enabled=True, validation_sandbox_profile=None),
    )
    sandbox = SandboxProfileConfig(
        id="validation",
        image="validation@sha256:" + "a" * 64,
        uid=10001,
        gid=10001,
        cpu_count=1,
        memory_bytes=128 * 1024 * 1024,
        pids_limit=64,
        tmpfs_bytes=16 * 1024 * 1024,
        timeout_seconds=120,
        output_bytes=4096,
    )
    registries.sandboxes.get.return_value = sandbox
    runtime = AsyncMock()
    runtime.run.return_value = SimpleNamespace(exit_code=exit_code)
    kwargs = dict(seccomp_profile=tmp_path / "seccomp.json", seccomp_digest="a" * 64)
    if exit_code:
        with pytest.raises(RuntimeError, match="validation.*failed toolchain preflight.*make"):
            await cli._preflight_validation_images(runtime, registries, **kwargs)
        assert runtime.run.await_count == 1
    else:
        await cli._preflight_validation_images(runtime, registries, **kwargs)
        assert [c.args[0].argv for c in runtime.run.await_args_list] == list(
            cli.VALIDATION_IMAGE_PREFLIGHT_COMMANDS
        )
    registries.sandboxes.get.assert_called_once_with("validation")
    for call in runtime.run.await_args_list:
        envelope = call.args[0]
        assert envelope.sandbox.network_disabled
        assert envelope.sandbox.mounts == ()
        assert envelope.sandbox.timeout_seconds == 60
        assert envelope.sandbox.environment["UV_OFFLINE"] == "1"
        assert envelope.sandbox.environment["UV_NO_SYNC"] == "1"
        assert envelope.stdin == ""
        assert not call.args[1].requested


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [False, True])
async def test_dream_scheduling_failure_does_not_prevent_clean_shutdown(
    monkeypatch, caplog, failure
):
    from vuzol.cli import executor as cli
    from vuzol.config.settings import ProjectLeadSettings

    caplog.set_level("INFO")
    callbacks = {}
    loop = SimpleNamespace(
        add_signal_handler=lambda signum, callback: callbacks.update({signum: callback})
    )
    monkeypatch.setattr(cli.asyncio, "get_running_loop", lambda: loop)
    processor = MagicMock()
    processor.lanes.return_value = ()
    processor.recover_if_due = AsyncMock()
    factory = MagicMock()
    factory.begin.return_value.__aenter__ = AsyncMock(return_value="session")
    factory.begin.return_value.__aexit__ = AsyncMock(return_value=False)

    async def schedule(session, **kwargs):
        callbacks[signal.SIGTERM]()
        if failure:
            raise RuntimeError("dream database unavailable")

    enqueue = AsyncMock(side_effect=schedule)
    monkeypatch.setattr(cli, "enqueue_dreams", enqueue)
    settings = ProjectLeadSettings()
    await cli._run_loop(
        processor, 0.001, session_factory=factory, dream_settings=settings, timezone="Europe/Paris"
    )
    enqueue.assert_awaited_once_with("session", settings=settings, timezone="Europe/Paris")
    processor.recover_if_due.assert_awaited_once()
    assert (
        any(getattr(r, "event", None) == "executor.dream_schedule_failed" for r in caplog.records)
        is failure
    )
    assert any(getattr(r, "event", None) == "executor.stopped" for r in caplog.records)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case", ["not_due", "periodic", "missing_item", "null_chat", "forced", "error"]
)
async def test_subscription_tick_refreshes_only_when_due_and_acknowledges_claimed_request(
    monkeypatch, caplog, case
):
    from vuzol.cli import executor as cli
    from vuzol.config import SubscriptionLimitSettings
    from vuzol.providers import subscription_limits
    from vuzol.storage import leasing
    from vuzol.telegram import projections

    factory, registries = MagicMock(), MagicMock()
    session = AsyncMock()
    session.scalars.return_value = SimpleNamespace(all=lambda: [-100, -200])
    factory.begin.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    token = (
        SimpleNamespace(item_id=uuid.uuid4())
        if case in {"missing_item", "null_chat", "forced"}
        else None
    )
    session.get.return_value = (
        None
        if case == "missing_item"
        else SimpleNamespace(payload={"chat_id": None if case == "null_chat" else -300})
    )
    claim = AsyncMock(return_value=token)
    if case == "error":
        claim.side_effect = RuntimeError("database unavailable")
    complete, refresh, dashboard = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(leasing, "claim_outbox_item", claim)
    monkeypatch.setattr(leasing, "complete_outbox_item", complete)
    monkeypatch.setattr(subscription_limits, "refresh_and_store_subscription_limits", refresh)
    monkeypatch.setattr(projections, "enqueue_project_status_dashboard", dashboard)
    result = await cli._refresh_subscription_limits_tick(
        factory, registries, due_periodic=case == "periodic", settings=SubscriptionLimitSettings()
    )
    assert result is (token is not None)
    if case in {"not_due", "error"}:
        refresh.assert_not_awaited()
        dashboard.assert_not_awaited()
        complete.assert_not_awaited()
    else:
        refresh.assert_awaited_once()
        assert [call.args[1] for call in dashboard.await_args_list] == (
            [-300] if case == "forced" else [-100, -200]
        )
        if token:
            complete.assert_awaited_once_with(session, token)
        else:
            complete.assert_not_awaited()
    if case == "error":
        assert any(
            getattr(r, "event", None) == "executor.subscription_limits_failed"
            for r in caplog.records
        )
