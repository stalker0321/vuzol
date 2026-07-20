"""Rootless runtime tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import (
    AsyncMock,
    CancellationContext,
    Path,
    RootlessDockerRuntime,
    RootlessIdentityResolver,
    SandboxError,
    WorktreeAccessError,
    _acl_has_named_user,
    _collect_entries,
    _map_id,
    _read_id_map,
    _require_trusted_command,
    asyncio,
    envelope,
    os,
    pytest,
)


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
