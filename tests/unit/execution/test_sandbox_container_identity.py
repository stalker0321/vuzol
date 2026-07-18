"""Sandbox container identity tests (split for cohesion)."""

from __future__ import annotations

from ._execution_helpers import (
    AsyncMock,
    MagicMock,
    Path,
    RootlessDockerRuntime,
    SandboxError,
    envelope,
    pytest,
)


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
