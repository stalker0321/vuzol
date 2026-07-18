"""Sandbox spec argv tests (split for cohesion)."""

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
