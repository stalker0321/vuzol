"""Domain tests split from the former monolithic test_execution module."""

from __future__ import annotations

from ._execution_helpers import *


def test_sandbox_spec_hash_is_stable_and_redacts_stdin(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    assert configured.stable_hash == configured.model_copy().stable_hash
    assert configured.stdin not in repr(configured.redacted)
    assert configured.sandbox.stable_hash in repr(configured.redacted)


def test_docker_argv_enforces_outer_isolation(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    argv = docker_run_argv(tmp_path / "docker.sock", "task", configured)
    rendered = " ".join(argv)
    assert "--network none" in rendered
    assert "--read-only" in argv
    assert "--rm" not in argv
    assert "--cap-drop ALL" in rendered
    assert "no-new-privileges:true" in argv
    assert "/var/run/docker.sock" not in rendered
    assert configured.sandbox.image in argv
    mount_specs = [argv[index + 1] for index, item in enumerate(argv) if item == "--mount"]
    assert len(mount_specs) == 2
    assert all(not spec.endswith((",ro", ",rw", ",readonly")) for spec in mount_specs)

    readonly_source = tmp_path / "state"
    readonly_source.mkdir()
    readonly_mount = SandboxMount(
        source=readonly_source,
        target=Path("/state"),
        mode=MountMode.READ_ONLY,
        purpose="provider-state",
    )
    readonly_envelope = configured.model_copy(
        update={
            "sandbox": configured.sandbox.model_copy(
                update={"mounts": (*configured.sandbox.mounts, readonly_mount)}
            )
        }
    )
    readonly_argv = docker_run_argv(tmp_path / "docker.sock", "task", readonly_envelope)
    assert f"type=bind,src={readonly_source},dst=/state,readonly" in readonly_argv


def test_seccomp_profile_validation_fails_closed(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(SandboxError, match="unavailable"):
        validate_seccomp_profile(missing, "0" * 64)

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(SandboxError, match="regular file"):
        validate_seccomp_profile(directory, "0" * 64)

    profile, digest = _seccomp_profile(tmp_path)
    symlink = tmp_path / "seccomp-link.json"
    symlink.symlink_to(profile)
    with pytest.raises(SandboxError, match="symlinks"):
        validate_seccomp_profile(symlink, digest)

    profile.chmod(0o622)
    with pytest.raises(SandboxError, match="mode is unsafe"):
        validate_seccomp_profile(profile, digest)

    profile.chmod(0o600)
    with pytest.raises(SandboxError, match="digest mismatch"):
        validate_seccomp_profile(profile, "0" * 64)


@pytest.mark.anyio
async def test_rootful_docker_socket_is_rejected() -> None:
    with pytest.raises(SandboxError, match="rootful"):
        await RootlessDockerRuntime(Path("/var/run/docker.sock")).preflight()


@pytest.mark.anyio
async def test_rootless_runtime_preflight_and_successful_bounded_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    socket = tmp_path / "docker.sock"
    socket.touch()
    runtime = RootlessDockerRuntime(socket)

    async def fake_docker(*args: str) -> str:
        assert args[0] == "info"
        if args[-1] == "{{.CgroupVersion}}":
            return "2"
        if args[-1] == "{{json .Warnings}}":
            return "[]"
        return '["name=rootless","name=seccomp"]'

    monkeypatch.setattr(runtime, "_docker", fake_docker)
    monkeypatch.setattr("vuzol.execution.sandbox.stat.S_ISSOCK", lambda _mode: True)
    await runtime.preflight()

    executable = tmp_path / "bin" / "docker"
    executable.parent.mkdir()
    executable.write_text("#!/bin/sh\ncat\n")
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{executable.parent}:{os.environ['PATH']}")
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]
    result = await runtime.run(envelope(tmp_path), CancellationContext())
    assert result.exit_code == 0 and result.stdout == "bounded prompt"
    runtime._remove_owned_container.assert_awaited_once()


@pytest.mark.anyio
async def test_rootless_preflight_rejects_missing_and_non_socket(tmp_path: Path) -> None:
    runtime = RootlessDockerRuntime(tmp_path / "missing.sock")
    with pytest.raises(SandboxError, match="unavailable"):
        await runtime.preflight()
    regular = tmp_path / "regular.sock"
    regular.touch()
    with pytest.raises(SandboxError, match="not a Unix socket"):
        await RootlessDockerRuntime(regular).preflight()


@pytest.mark.anyio
async def test_rootless_preflight_rejects_incomplete_security_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    socket = tmp_path / "docker.sock"
    socket.touch()
    runtime = RootlessDockerRuntime(socket)
    monkeypatch.setattr("vuzol.execution.sandbox.stat.S_ISSOCK", lambda _mode: True)
    runtime._docker = AsyncMock(return_value='["name=rootless"]')  # type: ignore[method-assign]
    with pytest.raises(SandboxError, match="seccomp"):
        await runtime.preflight()
    runtime._docker = AsyncMock(  # type: ignore[method-assign]
        side_effect=['["name=rootless","name=seccomp"]', "1"]
    )
    with pytest.raises(SandboxError, match="cgroup v2"):
        await runtime.preflight()
    runtime._docker = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            '["name=rootless","name=seccomp"]',
            "2",
            '["WARNING: No memory limit support"]',
        ]
    )
    with pytest.raises(SandboxError, match="required cgroup limits"):
        await runtime.preflight()


@pytest.mark.anyio
async def test_rootless_docker_command_failure_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "docker"
    executable.write_text("#!/bin/sh\necho denied >&2\nexit 2\n")
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path))
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    with pytest.raises(SandboxError, match="operation failed"):
        await runtime._docker("info")


@pytest.mark.anyio
async def test_rootless_runtime_timeout_reaps_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "docker"
    executable.write_text(
        '#!/bin/sh\ncase " $* " in\n  *" run "*) exec sleep 10 ;;\n  *) exit 0 ;;\nesac\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    base = envelope(tmp_path)
    configured = base.model_copy(
        update={"sandbox": base.sandbox.model_copy(update={"timeout_seconds": 1})}
    )
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._stop_owned_container = AsyncMock()  # type: ignore[method-assign]
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]
    with pytest.raises(SandboxError, match="timed out"):
        await runtime.run(configured, CancellationContext())
    runtime._remove_owned_container.assert_awaited_once()


@pytest.mark.anyio
async def test_rootless_runtime_output_limit_stops_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "docker"
    executable.write_text(
        "#!/bin/sh\n"
        'case " $* " in\n'
        '  *" run "*) cat >/dev/null; exec head -c 2000 /dev/zero ;;\n'
        "  *) exit 0 ;;\n"
        "esac\n"
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    base = envelope(tmp_path)
    configured = base.model_copy(
        update={"sandbox": base.sandbox.model_copy(update={"output_bytes": 100})}
    )
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._stop_owned_container = AsyncMock()  # type: ignore[method-assign]
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]
    with pytest.raises(SandboxError, match="output limit"):
        await runtime.run(configured, CancellationContext())
    runtime._remove_owned_container.assert_awaited_once()


@pytest.mark.anyio
async def test_rootless_runtime_external_task_cancellation_always_cleans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "docker"
    executable.write_text(
        '#!/bin/sh\ncase " $* " in\n  *" run "*) exec sleep 10 ;;\n  *) exit 0 ;;\nesac\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._stop_owned_container = AsyncMock()  # type: ignore[method-assign]
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]
    task = asyncio.create_task(runtime.run(envelope(tmp_path), CancellationContext()))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    runtime._stop_owned_container.assert_awaited_once()
    runtime._remove_owned_container.assert_awaited_once()


def test_grok_diagnostic_tar_accepts_one_bounded_regular_file() -> None:
    assert (
        _single_regular_tar_file(
            _tar_file("events.jsonl", b"safe"), expected_name="events.jsonl", maximum=4
        )
        == b"safe"
    )
    assert (
        _single_regular_tar_file(
            _tar_file("events.jsonl", b"unsafe", symlink=True),
            expected_name="events.jsonl",
            maximum=100,
        )
        is None
    )
    assert (
        _single_regular_tar_file(
            _tar_file("events.jsonl", b"large"), expected_name="events.jsonl", maximum=4
        )
        is None
    )
    assert (
        _single_regular_tar_file(
            _tar_file("auth.json", b"secret"), expected_name="events.jsonl", maximum=100
        )
        is None
    )
    assert _single_regular_tar_file(b"not a tar", expected_name="events.jsonl", maximum=100) is None


@pytest.mark.anyio
async def test_bounded_read_handles_absent_stream() -> None:
    assert await _bounded_read(None, 1) == b""


@pytest.mark.anyio
async def test_container_copy_accepts_only_bounded_exact_tar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = MagicMock(
        stdout=_reader(_tar_file("events.jsonl", b"safe")),
        stderr=_reader(b""),
    )
    good.wait = AsyncMock(return_value=0)
    missing = MagicMock(stdout=_reader(b""), stderr=_reader(b"missing"))
    missing.wait = AsyncMock(return_value=1)
    oversized = MagicMock(
        stdout=_reader(b"x" * (GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1_048_577)),
        stderr=_reader(b""),
    )
    oversized.wait = AsyncMock(return_value=0)
    oversized.kill = MagicMock()
    create = AsyncMock(side_effect=(good, missing, oversized))
    monkeypatch.setattr("vuzol.execution.sandbox.asyncio.create_subprocess_exec", create)
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")

    assert await runtime._copy_container_regular_file("a" * 64, "/exact/events.jsonl") == b"safe"
    assert await runtime._copy_container_regular_file("a" * 64, "/exact/events.jsonl") is None
    assert await runtime._copy_container_regular_file("a" * 64, "/exact/events.jsonl") is None
    oversized.kill.assert_called_once()


def test_diagnostic_staging_rejects_invalid_mounts_and_destinations(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    assert _artifact_staging(configured) == tmp_path / "artifacts"
    no_artifacts = configured.model_copy(
        update={
            "sandbox": configured.sandbox.model_copy(
                update={"mounts": configured.sandbox.mounts[:1]}
            )
        }
    )
    assert _artifact_staging(no_artifacts) is None
    missing_source = tmp_path / "missing-artifacts"
    missing_mount = configured.sandbox.mounts[1].model_copy(update={"source": missing_source})
    missing = configured.model_copy(
        update={
            "sandbox": configured.sandbox.model_copy(
                update={"mounts": (configured.sandbox.mounts[0], missing_mount)}
            )
        }
    )
    assert _artifact_staging(missing) is None

    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    paths = staged_grok_diagnostic_paths(tmp_path / "artifacts", session_id)
    assert paths is not None
    with pytest.raises(ValueError, match="bounded limit"):
        _atomic_write_diagnostic(
            tmp_path / "artifacts",
            paths[0],
            b"x" * (GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1),
        )
    with pytest.raises(ValueError, match="destination is invalid"):
        _atomic_write_diagnostic(tmp_path / "artifacts", tmp_path / "other", b"safe")

    paths[0].parent.mkdir(parents=True)
    paths[0].symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="destination is unsafe"):
        _prepare_diagnostic_destinations(tmp_path / "artifacts", paths)


def test_atomic_diagnostic_write_cleans_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    paths = staged_grok_diagnostic_paths(tmp_path, session_id)
    assert paths is not None
    monkeypatch.setattr("vuzol.execution.sandbox.os.write", lambda _fd, _content: 0)
    with pytest.raises(OSError, match="no progress"):
        _atomic_write_diagnostic(tmp_path, paths[0], b"safe")
    assert not list(paths[0].parent.glob("*.tmp"))


@pytest.mark.anyio
async def test_grok_staging_degrades_for_missing_identity_or_session(tmp_path: Path) -> None:
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    configured = envelope(tmp_path).model_copy(update={"argv": ("grok",)})
    runtime._owned_container_id = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runtime._copy_container_regular_file = AsyncMock()  # type: ignore[method-assign]
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    await runtime._stage_grok_diagnostics(
        "missing", configured, f'{{"type":"end","sessionId":"{session_id}"}}'
    )
    await runtime._stage_grok_diagnostics("missing", configured, '{"type":"end"}')
    runtime._copy_container_regular_file.assert_not_awaited()


@pytest.mark.anyio
async def test_rootless_runtime_stages_exact_grok_session_and_always_removes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    executable = tmp_path / "docker"
    executable.write_text(
        "#!/bin/sh\ncat >/dev/null\n"
        f'printf \'%s\\n\' \'{{"type":"end","stopReason":"EndTurn",'
        f'"sessionId":"{session_id}"}}\'\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    base = envelope(tmp_path)
    configured = base.model_copy(update={"argv": ("grok",)})
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._owned_container_id = AsyncMock(return_value="a" * 64)  # type: ignore[method-assign]
    runtime._copy_container_regular_file = AsyncMock(  # type: ignore[method-assign]
        side_effect=[b'{"type":"turn_started","schema_version":"1.0"}\n', b""]
    )
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]

    result = await runtime.run(configured, CancellationContext())

    assert result.exit_code == 0
    staged = staged_grok_diagnostic_paths(tmp_path / "artifacts", session_id)
    assert staged is not None
    assert staged[0].read_bytes().startswith(b'{"type"')
    assert staged[1].read_bytes() == b""
    requested = [call.args[1] for call in runtime._copy_container_regular_file.await_args_list]
    assert requested == [
        f"/grok-home/.grok/sessions/%2Fworkspace/{session_id}/events.jsonl",
        f"/grok-home/.grok/sessions/%2Fworkspace/{session_id}/updates.jsonl",
    ]
    runtime._remove_owned_container.assert_awaited_once()


@pytest.mark.anyio
async def test_grok_extraction_failure_preserves_result_and_removes_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    executable = tmp_path / "docker"
    executable.write_text(
        "#!/bin/sh\ncat >/dev/null\n"
        f'printf \'%s\\n\' \'{{"type":"end","stopReason":"EndTurn",'
        f'"sessionId":"{session_id}"}}\'\n'
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    configured = envelope(tmp_path).model_copy(update={"argv": ("grok",)})
    stale_paths = staged_grok_diagnostic_paths(tmp_path / "artifacts", session_id)
    assert stale_paths is not None
    stale_paths[0].parent.mkdir(parents=True)
    stale_paths[0].write_text("stale evidence")
    stale_paths[1].write_text("stale evidence")
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._owned_container_id = AsyncMock(return_value="a" * 64)  # type: ignore[method-assign]
    runtime._copy_container_regular_file = AsyncMock(  # type: ignore[method-assign]
        side_effect=SandboxError("copy failed")
    )
    runtime._remove_owned_container = AsyncMock()  # type: ignore[method-assign]

    result = await runtime.run(configured, CancellationContext())

    assert result.exit_code == 0
    assert not stale_paths[0].exists()
    assert not stale_paths[1].exists()
    runtime._remove_owned_container.assert_awaited_once()


@pytest.mark.anyio
async def test_foreign_container_is_never_removed(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")
    runtime._owned_container_id = AsyncMock(return_value=None)  # type: ignore[method-assign]
    runtime._docker = AsyncMock()  # type: ignore[method-assign]

    await runtime._remove_owned_container("foreign", configured)
    runtime._docker.assert_not_awaited()


@pytest.mark.anyio
async def test_owned_container_lookup_requires_name_and_all_identity_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = envelope(tmp_path)
    process = MagicMock(returncode=0)
    process.communicate = AsyncMock(return_value=(("a" * 64 + "\n").encode(), b""))
    create = AsyncMock(return_value=process)
    monkeypatch.setattr("vuzol.execution.sandbox.asyncio.create_subprocess_exec", create)

    container_id = await RootlessDockerRuntime(tmp_path / "docker.sock")._owned_container_id(
        "expected-name", configured
    )

    assert container_id == "a" * 64
    call = create.await_args
    assert call is not None
    arguments = call.args
    rendered = " ".join(str(value) for value in arguments)
    assert "name=^/expected-name$" in rendered
    for key, value in (
        ("task_id", configured.task_id),
        ("run_id", configured.run_id),
        ("step_id", configured.step_id),
        ("lease_generation", configured.lease_generation),
    ):
        assert f"label=vuzol.{key}={value}" in rendered


@pytest.mark.anyio
async def test_owned_container_lookup_and_cleanup_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured = envelope(tmp_path)
    failed = MagicMock(returncode=1)
    failed.communicate = AsyncMock(return_value=(b"", b"failed"))
    absent = MagicMock(returncode=0)
    absent.communicate = AsyncMock(return_value=(b"", b""))
    malformed = MagicMock(returncode=0)
    malformed.communicate = AsyncMock(return_value=(b"short\n", b""))
    create = AsyncMock(side_effect=(failed, absent, malformed))
    monkeypatch.setattr("vuzol.execution.sandbox.asyncio.create_subprocess_exec", create)
    runtime = RootlessDockerRuntime(tmp_path / "docker.sock")

    with pytest.raises(SandboxError, match="lookup failed"):
        await runtime._owned_container_id("expected", configured)
    assert await runtime._owned_container_id("expected", configured) is None
    with pytest.raises(SandboxError, match="identity is malformed"):
        await runtime._owned_container_id("expected", configured)

    runtime._owned_container_id = AsyncMock(  # type: ignore[method-assign]
        side_effect=("a" * 64, "a" * 64)
    )
    runtime._docker = AsyncMock(  # type: ignore[method-assign]
        side_effect=(SandboxError("stop failed"), "", "")
    )
    await runtime._stop_owned_container("expected", configured)
    await runtime._remove_owned_container("expected", configured)
    assert [call.args[0] for call in runtime._docker.await_args_list] == [
        "stop",
        "kill",
        "rm",
    ]


def test_grok_summary_uses_only_exact_bounded_staged_session(tmp_path: Path) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    stale_id = "019f5e8d-d90b-7e40-a698-8a71fa87eff8"
    for current, decision in ((session_id, "allow"), (stale_id, "cancelled")):
        paths = staged_grok_diagnostic_paths(tmp_path, current)
        assert paths is not None
        paths[0].parent.mkdir(parents=True)
        paths[0].write_text(
            "\n".join(
                (
                    '{"type":"turn_started","schema_version":"1.0"}',
                    '{"type":"tool_started","tool_name":"run_terminal_command"}',
                    '{"type":"permission_requested","tool_name":"run_terminal_command"}',
                    f'{{"type":"permission_resolved","decision":"{decision}"}}',
                    '{"type":"tool_completed","outcome":"success"}',
                )
            )
        )
        paths[1].write_text(
            json.dumps(
                {
                    "method": "session/update",
                    "params": {
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": f"call-{current}-31",
                            "rawInput": {"command": "make test"},
                            "_meta": {"x.ai/tool": {"name": "run_terminal_command"}},
                        }
                    },
                }
            )
        )
    stdout = f'{{"type":"end","stopReason":"EndTurn","sessionId":"{session_id}"}}'
    summary = _summarize_grok_process(stdout, tmp_path)
    assert summary["last_permission_decision"] == "allowed"
    assert summary["last_safe_command_identity"] == "make test"
    assert summary["last_tool_result_received"] is True
    assert summary["evidence_completeness"] == "complete"

    exact_paths = staged_grok_diagnostic_paths(tmp_path, session_id)
    assert exact_paths is not None
    assert not exact_paths[0].exists() and not exact_paths[1].exists()
    exact_paths[0].parent.mkdir(parents=True)
    exact_paths[0].write_text(
        "\n".join(
            (
                '{"type":"turn_started","schema_version":"1.0"}',
                '{"type":"tool_started","tool_name":"run_terminal_command"}',
            )
        )
    )
    partial = _summarize_grok_process(stdout, tmp_path)
    assert partial["evidence_completeness"] == "partial"
    exact_paths[0].parent.mkdir(parents=True)
    exact_paths[1].write_text("{}")
    missing_events = _summarize_grok_process(stdout, tmp_path)
    assert missing_events["evidence_completeness"] == "unavailable"
    unavailable = _summarize_grok_process(stdout, tmp_path)
    assert unavailable["evidence_completeness"] == "unavailable"


def test_grok_summary_rejects_oversized_or_symlinked_staged_diagnostics(
    tmp_path: Path,
) -> None:
    session_id = "019f6149-44c0-7520-932c-5e0f41c99351"
    paths = staged_grok_diagnostic_paths(tmp_path, session_id)
    assert paths is not None
    paths[0].parent.mkdir(parents=True)
    paths[0].write_bytes(b"x" * (GROK_DIAGNOSTIC_FILE_MAX_BYTES + 1))
    paths[1].symlink_to(paths[0])
    stdout = f'{{"type":"end","stopReason":"Cancelled","sessionId":"{session_id}"}}'
    summary = _summarize_grok_process(stdout, tmp_path)
    assert summary["evidence_completeness"] == "unavailable"
    assert summary["cancellation_evidence_category"] == "PROVIDER_CANCELLED_UNATTRIBUTED"


def test_rootless_identity_mapping_uses_active_namespace_files(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    socket = root / "docker.sock"
    socket.touch()
    pid_file = root / "docker.pid"
    pid_file.write_text("42")
    pid_file.chmod(0o600)
    process = root / "proc" / "42"
    (process / "ns").mkdir(parents=True)
    (process / "cmdline").write_bytes(b"dockerd\0" + os.fsencode(f"--host=unix://{socket}") + b"\0")
    (process / "uid_map").write_text(f"0 {os.geteuid()} 1\n1 100000 65536\n")
    (process / "gid_map").write_text(f"0 {os.getegid()} 1\n1 100000 65536\n")
    (process / "ns" / "user").touch()
    resolver = RootlessIdentityResolver(
        socket,
        proc_root=root / "proc",
        pid_file=pid_file,
    )
    identity = resolver.resolve(10_001, 10_001)
    assert identity.host_uid == 110_000
    assert identity.host_gid == 110_000
    assert identity.namespace_pid == 42
    with pytest.raises(WorktreeAccessError, match="non-root"):
        resolver.resolve(0, 10_001)
    with pytest.raises(WorktreeAccessError, match="PID file is unavailable"):
        RootlessIdentityResolver(socket, pid_file=root / "missing.pid").resolve(10_001, 10_001)
    pid_file.write_text("0")
    with pytest.raises(WorktreeAccessError, match="PID is invalid"):
        resolver.resolve(10_001, 10_001)
    pid_file.write_text("42")
    (process / "cmdline").write_bytes(b"python\0")
    with pytest.raises(WorktreeAccessError, match="does not identify dockerd"):
        resolver.resolve(10_001, 10_001)
    (process / "cmdline").write_bytes(b"dockerd\0--host=unix:///wrong.sock\0")
    with pytest.raises(WorktreeAccessError, match="does not own"):
        resolver.resolve(10_001, 10_001)
    (process / "cmdline").write_bytes(b"dockerd\0" + os.fsencode(f"--host=unix://{socket}") + b"\0")
    (process / "uid_map").write_text("0 99999 1\n1 100000 65536\n")
    with pytest.raises(WorktreeAccessError, match="root does not map"):
        resolver.resolve(10_001, 10_001)
    (process / "uid_map").write_text(f"0 {os.geteuid()} 1\n10001 {os.geteuid()} 1\n")
    with pytest.raises(WorktreeAccessError, match="unexpectedly maps"):
        resolver.resolve(10_001, 10_001)
    (process / "cmdline").unlink()
    with pytest.raises(WorktreeAccessError, match="namespace is unavailable"):
        resolver.resolve(10_001, 10_001)
    pid_file.chmod(0o666)
    with pytest.raises(WorktreeAccessError, match="PID file is unsafe"):
        resolver.resolve(10_001, 10_001)


def test_rootless_mapping_and_acl_helpers_fail_closed(tmp_path: Path) -> None:
    mapping = tmp_path / "mapping"
    mapping.write_text("")
    with pytest.raises(WorktreeAccessError, match="empty"):
        _read_id_map(mapping)
    mapping.write_text("not a mapping\n")
    with pytest.raises(WorktreeAccessError, match="malformed"):
        _read_id_map(mapping)
    mapping.write_text("0 1000 0\n")
    with pytest.raises(WorktreeAccessError, match="empty range"):
        _read_id_map(mapping)
    with pytest.raises(WorktreeAccessError, match="no unique"):
        _map_id(((0, 1000, 1),), 10_001)
    with pytest.raises(WorktreeAccessError, match="malformed"):
        _acl_has_named_user(b"bad", 60_001)

    missing = tmp_path / "missing-command"
    with pytest.raises(WorktreeAccessError, match="unavailable"):
        _require_trusted_command(missing)
    unsafe = tmp_path / "unsafe-command"
    unsafe.write_text("binary")
    unsafe.chmod(0o777)
    with pytest.raises(WorktreeAccessError, match="unsafe"):
        _require_trusted_command(unsafe)
    with pytest.raises(WorktreeAccessError, match="unavailable"):
        _collect_entries(tmp_path / "missing-worktree")
    link = tmp_path / "root-link"
    link.symlink_to(tmp_path)
    with pytest.raises(WorktreeAccessError, match="contained regular directory"):
        _collect_entries(link)


@pytest.mark.anyio
async def test_missing_validation_sandbox_prevents_provider_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.providers import handlers as provider_handlers
    from vuzol.providers.handlers import ProviderStepHandler

    monkeypatch.setattr(provider_handlers, "release_reservation", AsyncMock())

    factory = MagicMock()
    handler = ProviderStepHandler(
        factory,
        MagicMock(),
        MagicMock(),
        worktrees=MagicMock(),
        finalizer=MagicMock(),
        worktree_access=MagicMock(),
    )
    provider_request = MagicMock(task_draft={"step09a_capsule": {}})
    handler._build_request = AsyncMock(  # type: ignore[method-assign]
        return_value=(provider_request, "grok-a", uuid.uuid4(), "revision")
    )
    handler._grant_worktree_access = AsyncMock(  # type: ignore[method-assign]
        side_effect=WorktreeAccessError("project has no validation sandbox profile")
    )
    handler._unwind_pre_provider = AsyncMock()  # type: ignore[method-assign]
    handler._execute_built = AsyncMock()  # type: ignore[method-assign]

    outcome = await handler.execute(MagicMock(step_type="execute_code"), CancellationContext())

    assert outcome.category == "worker_access_unavailable"
    handler._execute_built.assert_not_awaited()


@pytest.mark.anyio
async def test_gate_runner_requires_fenced_sandbox_context(tmp_path: Path) -> None:
    runner, envelopes, runtime = _sandbox_gate_runner()
    with pytest.raises(ValueError, match="context is unavailable"):
        await runner.run(
            tmp_path,
            (RequiredGate(name="test", command_id="make test"),),
            timeout_seconds=10,
            context=None,
            cancellation=None,
        )
    envelopes.build_gate.assert_not_awaited()
    runtime.run.assert_not_awaited()


def test_worker_finalizer_requires_explicit_sandbox_runner() -> None:
    with pytest.raises(ValueError, match="explicit sandbox gate runner"):
        WorkerFinalizer(LocalGit())


@pytest.mark.anyio
async def test_sandbox_codex_transport_records_success_and_failure(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    process_id = uuid.uuid4()
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=())
    envelopes.build = AsyncMock(return_value=(configured, process_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    runtime = MagicMock()
    runtime.run = AsyncMock(return_value=CodexProcessResult(0, "ok", "", 5))
    transport = SandboxCodexTransport(runtime, envelopes, MagicMock())

    result = await transport.run(MagicMock(), CancellationContext())
    assert result.stdout == "ok"
    envelopes.mark_running.assert_awaited_once()
    envelopes.complete.assert_awaited_once()

    runtime.run.side_effect = SandboxError("failed after start")
    with pytest.raises(SandboxError):
        await transport.run(MagicMock(), CancellationContext())
    envelopes.fail_unknown.assert_awaited_once_with(process_id)


@pytest.mark.anyio
async def test_sandbox_transport_materializes_and_cleans_controlled_proxy(
    tmp_path: Path,
) -> None:
    configured = envelope(tmp_path)
    process_id = uuid.uuid4()
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    target = MagicMock()
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(target,))
    envelopes.build = AsyncMock(return_value=(configured, process_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    runtime = MagicMock()
    runtime.run = AsyncMock(return_value=CodexProcessResult(0, "ok", "", 5))
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    lease = ProxyServiceLease(
        container_name="vuzol-proxy",
        networks=networks,
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
        policy_hash="a" * 64,
    )
    proxy = MagicMock()
    proxy.create = AsyncMock(return_value=lease)
    never_dead = asyncio.Event()

    async def wait_until_dead(_lease: ProxyServiceLease) -> None:
        await never_dead.wait()

    proxy.wait_until_dead = AsyncMock(side_effect=wait_until_dead)
    proxy.cleanup = AsyncMock()

    result = await SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy).run(
        invocation, CancellationContext()
    )
    assert result.stdout == "ok"
    proxy.create.assert_awaited_once_with(
        configured.task_id,
        configured.run_id,
        configured.step_id,
        configured.lease_generation,
        (target,),
    )
    envelopes.build.assert_awaited_once_with(
        invocation,
        proxy_network="vuzol-internal",
        https_proxy_url="http://vuzol-proxy:8888",
    )
    proxy.cleanup.assert_awaited_once_with(lease)


@pytest.mark.anyio
async def test_proxy_death_cancels_sandbox_and_fails_closed(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    process_id = uuid.uuid4()
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(MagicMock(),))
    envelopes.build = AsyncMock(return_value=(configured, process_id))
    envelopes.mark_running = AsyncMock()
    envelopes.complete = AsyncMock()
    envelopes.fail_unknown = AsyncMock()
    runtime = MagicMock()
    runtime_started = asyncio.Event()

    async def running(*_args: object) -> CodexProcessResult:
        runtime_started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled sandbox must not return")

    runtime.run = AsyncMock(side_effect=running)
    networks = ProxyNetworkLease(
        internal_name="vuzol-internal",
        egress_name="vuzol-egress",
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    lease = ProxyServiceLease(
        container_name="vuzol-proxy",
        networks=networks,
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
        policy_hash="a" * 64,
    )
    proxy = MagicMock()
    proxy.create = AsyncMock(return_value=lease)

    async def dies(_lease: ProxyServiceLease) -> None:
        await runtime_started.wait()

    proxy.wait_until_dead = AsyncMock(side_effect=dies)
    proxy.cleanup = AsyncMock()
    with pytest.raises(RuntimeError, match="proxy exited"):
        await SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy).run(
            invocation, CancellationContext()
        )
    proxy.cleanup.assert_awaited_once_with(lease)
    envelopes.fail_unknown.assert_awaited_once_with(process_id)


@pytest.mark.anyio
async def test_proxy_start_failure_prevents_sandbox_build_and_start(tmp_path: Path) -> None:
    configured = envelope(tmp_path)
    invocation = MagicMock(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        lease_generation=configured.lease_generation,
    )
    envelopes = MagicMock()
    envelopes.proxy_targets = AsyncMock(return_value=(MagicMock(),))
    envelopes.build = AsyncMock()
    proxy = MagicMock()
    proxy.create = AsyncMock(side_effect=ProxyServiceError("startup failed"))
    runtime = MagicMock()
    runtime.run = AsyncMock()
    with pytest.raises(ProxyServiceError, match="startup failed"):
        await SandboxCodexTransport(runtime, envelopes, MagicMock(), proxy).run(
            invocation, CancellationContext()
        )
    envelopes.build.assert_not_awaited()
    runtime.run.assert_not_awaited()


@pytest.mark.anyio
async def test_sandbox_preflight_and_argv_edges(tmp_path: Path) -> None:
    """Test preflight rejection and argv construction (real code paths)."""
    with pytest.raises(SandboxError, match="rootful"):
        await RootlessDockerRuntime(Path("/var/run/docker.sock")).preflight()

    # argv construction covers network, limits, mounts, env
    work = tmp_path / "w"
    art = tmp_path / "a"
    work.mkdir()
    art.mkdir()
    spec = SandboxSpec(
        image="ex@sha256:" + "b" * 64,
        uid=10001,
        gid=10001,
        seccomp_profile=_seccomp_profile(tmp_path)[0],
        seccomp_profile_sha256=_seccomp_profile(tmp_path)[1],
        working_directory=Path("/ws"),
        mounts=(
            SandboxMount(source=work, target=Path("/ws"), mode=MountMode.READ_WRITE, purpose="w"),
            SandboxMount(source=art, target=Path("/a"), mode=MountMode.READ_WRITE, purpose="a"),
        ),
        cpu_count=1.0,
        memory_bytes=64 * 1024 * 1024,
        pids_limit=10,
        tmpfs_bytes=10 * 1024 * 1024,
        open_files_limit=100,
        output_bytes=1000,
        timeout_seconds=5,
        stop_grace_seconds=1,
        network_disabled=False,
        proxy_network="vuzol-internal",
        https_proxy_url="http://vuzol-proxy:8888",
        environment={"FOO": "bar"},
    )
    env = ProcessEnvelope(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        worktree_id=uuid.uuid4(),
        profile_id="p",
        provider_attempt=1,
        lease_generation=1,
        argv=("codex",),
        stdin="hi",
        sandbox=spec,
    )
    argv = docker_run_argv(tmp_path / "sock", "c1", env)
    argv_str = " ".join(argv)
    assert "-i" in argv
    assert "--network" in argv_str
    assert "--mount" in argv_str
    assert "FOO=bar" in argv_str
    assert spec.image in argv
    assert "--network vuzol-internal" in argv_str
    for key in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        assert f"{key}=http://vuzol-proxy:8888" in argv
    for key in ("ALL_PROXY", "NO_PROXY", "all_proxy", "no_proxy"):
        assert f"{key}=" in argv
