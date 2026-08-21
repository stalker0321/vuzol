import asyncio
import ctypes
import io
import json
import os
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from vuzol.security import landlock
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.runtime_preview import (
    PreviewExportTooLarge,
    PreviewMaterializationError,
    PreviewRuntimeRegistry,
    RuntimePreviewHandler,
    RuntimeTarget,
    _export_commit,
    _free_loopback_port,
    _git_common_dir,
    _wait_until_ready,
    _web_component,
    cleanup_orphaned_runtimes,
)


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeArchiveProcess:
    def __init__(self, payload: bytes, status: int = 0, stderr: bytes = b"") -> None:
        self.stdout = io.BytesIO(payload)
        self.stderr = io.BytesIO(stderr)
        self._status = status
        self.killed = False

    def wait(self, timeout: float | None = None) -> int:
        return self._status

    def kill(self) -> None:
        self.killed = True


def _request() -> StepExecutionRequest:
    return cast(
        StepExecutionRequest,
        SimpleNamespace(task_id="task", run_id="run"),
    )


def _handler(
    tmp_path: Path, *, max_bytes: int = 10_000_000, max_files: int = 1_000
) -> tuple[RuntimePreviewHandler, MagicMock, MagicMock]:
    read_session = MagicMock()
    write_session = MagicMock()
    factory = MagicMock(return_value=AsyncContext(read_session))
    factory.begin.return_value = AsyncContext(write_session)
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            preview_site_root=tmp_path / "previews",
            preview_site_base_url="https://test.example",
            repository_root=tmp_path / "repositories",
            preview_export_max_bytes=max_bytes,
            preview_export_max_files=max_files,
            capability_provisioning=SimpleNamespace(
                toolchain_root=tmp_path / "toolchains",
            ),
        )
    )
    handler = RuntimePreviewHandler(
        cast(Any, factory),
        cast(Any, runtime),
        PreviewRuntimeRegistry(),
    )
    return handler, read_session, write_session


def _environment(
    *,
    command: list[str] | None = None,
    capabilities: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        contract={
            "components": {
                "web": {
                    "kind": "web_service",
                    "run_command": command if command is not None else ["node", "server.js"],
                    "healthcheck_path": "/ready",
                }
            },
            "capabilities": capabilities
            if capabilities is not None
            else {
                "node-runtime": {
                    "label": "Node.js runtime",
                    "provisioning": "automatic",
                }
            },
        }
    )


def _tar_bytes(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, content in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def _mock_materialization(
    monkeypatch: pytest.MonkeyPatch,
    files: dict[str, bytes] | None = None,
    *,
    status: int = 0,
    stderr: bytes = b"",
) -> MagicMock:
    import vuzol.workflows.runtime_preview as runtime_module

    payload = _tar_bytes(files if files is not None else {"server.js": b"console.log(1);"})
    spawn = MagicMock(return_value=FakeArchiveProcess(payload, status=status, stderr=stderr))
    monkeypatch.setattr(runtime_module, "_start_git_archive", spawn)

    async def inline_thread(func: Any, /, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", inline_thread)
    return spawn


def _allow_confinement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(landlock, "landlock_abi_version", lambda: 8)


def _prepare_git_pointer(tmp_path: Path) -> None:
    common = tmp_path / "repositories" / "demo" / ".git" / "worktrees" / "abc"
    common.mkdir(parents=True)
    (tmp_path / ".git").write_text(f"gitdir: {common}\n", encoding="utf-8")


def _mock_healthy_spawn(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    process = SimpleNamespace(
        returncode=None,
        terminate=MagicMock(),
        kill=MagicMock(),
        wait=AsyncMock(),
    )
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    return spawn


@pytest.mark.anyio
async def test_runtime_preview_publishes_healthy_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, read_session, write_session = _handler(tmp_path)
    read_session.get = AsyncMock(return_value=SimpleNamespace(project_id="falling-worlds"))
    read_session.scalar = AsyncMock(
        side_effect=[
            SimpleNamespace(path=str(tmp_path), result_commit="a" * 40),
            SimpleNamespace(work_package_id="package"),
        ]
    )
    write_session.get = AsyncMock(return_value=SimpleNamespace(preview_url=None))
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    monkeypatch.setattr(runtime_module, "_free_loopback_port", lambda: 43210)
    monkeypatch.setattr(
        runtime_module,
        "_wait_until_ready",
        AsyncMock(return_value={"status_code": 200, "path": "/ready"}),
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    archive = _mock_materialization(monkeypatch, {"server.js": b"console.log(1);"})
    spawn = _mock_healthy_spawn(monkeypatch)

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "published"
    assert outcome.result["public_url"] == "https://test.example/falling-worlds/"
    assert write_session.get.return_value.preview_url == outcome.result["public_url"]
    archive.assert_called_once()
    target = handler._registry.targets["falling-worlds"]
    assert target.port == 43210
    assert target.runtime_dir is not None
    assert (target.runtime_dir / "app" / "server.js").read_bytes() == b"console.log(1);"
    assert (target.runtime_dir / "home").is_dir()
    assert (target.runtime_dir / "tmp").is_dir()
    assert spawn.await_args is not None
    argv = spawn.await_args.args
    assert argv[0] == sys.executable
    assert argv[1] == str(runtime_module._wrapper_path())
    spec = json.loads(argv[2])
    assert spec["read_write"] == [str(target.runtime_dir)]
    assert argv[3] == "--"
    assert argv[4] == "/usr/bin/node"
    kwargs = spawn.await_args.kwargs
    assert kwargs["env"]["HOST"] == "127.0.0.1"
    assert kwargs["env"]["PORT"] == "43210"
    assert kwargs["env"]["HOME"] == str(target.runtime_dir / "home")
    assert kwargs["env"]["TMPDIR"] == str(target.runtime_dir / "tmp")
    assert kwargs["cwd"] == str(target.runtime_dir / "app")
    await handler._registry.close()
    assert target.process.terminate.call_count == 1  # type: ignore[attr-defined]


@pytest.mark.anyio
async def test_runtime_preview_reports_capability_setup_without_spawning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="android-api"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    environment = _environment(
        capabilities={"external-token": {"label": "API token", "provisioning": "external_setup"}}
    )
    monkeypatch.setattr(runtime_module, "current_environment", AsyncMock(return_value=environment))
    spawn = AsyncMock()
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "environment_setup_required"
    assert outcome.result["status"] == "needs_setup"
    assert outcome.result["capabilities"][0]["key"] == "external-token"
    spawn.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("task", "environment", "worktree", "expected"),
    [
        (None, None, None, "project_missing"),
        (
            SimpleNamespace(project_id="library"),
            SimpleNamespace(contract={"components": {}, "capabilities": {}}),
            SimpleNamespace(path="/worktree", result_commit="a" * 40),
            "no_web_component",
        ),
    ],
)
async def test_runtime_preview_skips_inapplicable_projects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    task: object,
    environment: object,
    worktree: object,
    expected: str,
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=task)
    session.scalar = AsyncMock(side_effect=[worktree, None])
    monkeypatch.setattr(runtime_module, "current_environment", AsyncMock(return_value=environment))

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.result["reason"] == expected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("command", "commit", "category"),
    [
        ([], "a" * 40, "environment_setup_required"),
        (["ruby", "app.rb"], "a" * 40, "environment_setup_required"),
        (["node", "server.js"], None, "preview_source_missing"),
        (["node", "server.js"], "not-a-commit", "preview_source_missing"),
    ],
)
async def test_runtime_preview_rejects_invalid_runtime_contracts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
    commit: str | None,
    category: str,
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit=commit), None]
    )
    monkeypatch.setattr(
        runtime_module,
        "current_environment",
        AsyncMock(return_value=_environment(command=command, capabilities={})),
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == category


@pytest.mark.anyio
async def test_runtime_preview_fails_closed_without_landlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    monkeypatch.setattr(landlock, "landlock_abi_version", lambda: 0)
    archive = _mock_materialization(monkeypatch)
    spawn = _mock_healthy_spawn(monkeypatch)

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_confinement_unavailable"
    archive.assert_not_called()
    spawn.assert_not_awaited()


@pytest.mark.anyio
async def test_runtime_preview_blocks_oversized_export_by_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path, max_bytes=5)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    _mock_materialization(monkeypatch, {"server.js": b"x" * 32})
    spawn = _mock_healthy_spawn(monkeypatch)

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_export_too_large"
    spawn.assert_not_awaited()
    assert not list((tmp_path / "previews" / ".runtime").glob("staging/*"))
    assert not list((tmp_path / "previews" / ".runtime").glob("runs/*"))


@pytest.mark.anyio
async def test_runtime_preview_blocks_export_with_too_many_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path, max_files=1)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    _mock_materialization(monkeypatch, {"one.js": b"1", "two.js": b"2"})

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_export_too_large"


@pytest.mark.anyio
async def test_runtime_preview_blocks_traversing_archive_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    _mock_materialization(monkeypatch, {"../escape.js": b"evil"})

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_materialization_failed"
    assert not (tmp_path / "previews" / "escape.js").exists()


@pytest.mark.anyio
async def test_runtime_preview_blocks_failed_git_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    _mock_materialization(monkeypatch, {}, status=128, stderr=b"fatal: not a valid object name")

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_materialization_failed"
    assert "not a valid object name" in (outcome.summary or "")


@pytest.mark.anyio
async def test_runtime_preview_maps_confinement_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    _mock_materialization(monkeypatch)
    process = SimpleNamespace(returncode=98, terminate=MagicMock(), wait=AsyncMock())
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_confinement_unavailable"
    assert outcome.result["log_path"].endswith("preview.log")


@pytest.mark.anyio
async def test_registry_replaces_and_closes_processes() -> None:
    first = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )
    second = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )
    registry = PreviewRuntimeRegistry()
    await registry.replace(
        RuntimeTarget("demo", 1, cast(asyncio.subprocess.Process, first), "a" * 40)
    )
    await registry.replace(
        RuntimeTarget("demo", 2, cast(asyncio.subprocess.Process, second), "b" * 40)
    )

    assert first.terminate.call_count == 1
    await registry.close()
    assert second.terminate.call_count == 1
    assert registry.targets == {}


@pytest.mark.anyio
async def test_registry_removes_previous_runtime_dir_on_replace(tmp_path: Path) -> None:
    previous_dir = tmp_path / "run-1"
    previous_dir.mkdir()
    replacement_dir = tmp_path / "run-2"
    replacement_dir.mkdir()
    previous = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )
    replacement = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )
    registry = PreviewRuntimeRegistry()

    await registry.replace(
        RuntimeTarget("demo", 1, cast(Any, previous), "a" * 40, runtime_dir=previous_dir)
    )
    await registry.replace(
        RuntimeTarget("demo", 2, cast(Any, replacement), "b" * 40, runtime_dir=replacement_dir)
    )

    assert not previous_dir.exists()
    assert replacement_dir.exists()
    await registry.close()
    assert not replacement_dir.exists()


@pytest.mark.anyio
async def test_runtime_preview_honors_cancellation(tmp_path: Path) -> None:
    handler, _session, _write = _handler(tmp_path)
    cancellation = CancellationContext()
    cancellation.request()

    outcome = await handler.execute(_request(), cancellation)

    assert outcome.kind is OutcomeKind.CANCELLED


@pytest.mark.anyio
async def test_runtime_preview_stops_unhealthy_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import vuzol.workflows.runtime_preview as runtime_module

    handler, session, _write = _handler(tmp_path)
    session.get = AsyncMock(return_value=SimpleNamespace(project_id="demo"))
    session.scalar = AsyncMock(
        side_effect=[SimpleNamespace(path=str(tmp_path), result_commit="a" * 40), None]
    )
    monkeypatch.setattr(
        runtime_module, "current_environment", AsyncMock(return_value=_environment())
    )
    _allow_confinement(monkeypatch)
    _prepare_git_pointer(tmp_path)
    _mock_materialization(monkeypatch)
    process = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    monkeypatch.setattr(
        runtime_module,
        "_wait_until_ready",
        AsyncMock(side_effect=TimeoutError("not ready")),
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_runtime_unhealthy"
    assert outcome.result["log_path"].endswith("preview.log")
    assert process.terminate.call_count == 1
    process.wait.assert_awaited()


@pytest.mark.anyio
async def test_healthcheck_retries_http_error_then_accepts_non_server_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.get.side_effect = [httpx.ConnectError("starting"), SimpleNamespace(status_code=404)]
    monkeypatch.setattr(
        "vuzol.workflows.runtime_preview.httpx.AsyncClient", lambda **_kwargs: client
    )
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    process = cast(asyncio.subprocess.Process, SimpleNamespace(returncode=None))

    result = await _wait_until_ready(process, port=1234, path="/ready")

    assert result == {"status_code": 404, "path": "/ready"}
    assert client.get.await_count == 2


@pytest.mark.anyio
async def test_healthcheck_fails_immediately_when_process_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    monkeypatch.setattr(
        "vuzol.workflows.runtime_preview.httpx.AsyncClient", lambda **_kwargs: client
    )
    process = cast(asyncio.subprocess.Process, SimpleNamespace(returncode=2))

    with pytest.raises(RuntimeError, match="code 2"):
        await _wait_until_ready(process, port=1234, path="/")


@pytest.mark.anyio
async def test_registry_kills_processes_that_ignore_termination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )
    remaining = SimpleNamespace(
        returncode=None, terminate=MagicMock(), kill=MagicMock(), wait=AsyncMock()
    )

    async def wait_for(awaitable: Any, **_kwargs: object) -> None:
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", wait_for)
    registry = PreviewRuntimeRegistry(
        {"demo": RuntimeTarget("demo", 1, cast(Any, previous), "a" * 40)}
    )

    await registry.replace(RuntimeTarget("demo", 2, cast(Any, remaining), "b" * 40))
    await registry.close()

    previous.kill.assert_called_once()
    remaining.kill.assert_called_once()
    previous.wait.assert_awaited_once()
    remaining.wait.assert_awaited_once()


@pytest.mark.anyio
async def test_healthcheck_times_out_after_only_server_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.__aexit__.return_value = None
    client.get.return_value = SimpleNamespace(status_code=503)
    monkeypatch.setattr(
        "vuzol.workflows.runtime_preview.httpx.AsyncClient", lambda **_kwargs: client
    )
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    process = cast(asyncio.subprocess.Process, SimpleNamespace(returncode=None))

    with pytest.raises(TimeoutError, match="timed out"):
        await _wait_until_ready(process, port=1234, path="/ready")

    assert client.get.await_count == 30


def test_runtime_preview_helpers_reject_malformed_components() -> None:
    assert _web_component(None) is None
    assert _web_component({"components": []}) is None
    assert _web_component({"components": {"bad": [], "cli": {"kind": "cli"}}}) is None
    assert 0 < _free_loopback_port() <= 65535


def _make_worktree_fixture(tmp_path: Path) -> tuple[Path, Path]:
    repository = tmp_path / "repositories" / "demo"
    common = repository / ".git"
    (common / "worktrees" / "abc").mkdir(parents=True)
    worktree = tmp_path / "worktrees" / "demo" / "run"
    worktree.mkdir(parents=True)
    return repository, worktree


def test_git_common_dir_resolves_linked_worktree(tmp_path: Path) -> None:
    repository, worktree = _make_worktree_fixture(tmp_path)
    pointer = repository / ".git" / "worktrees" / "abc"
    (worktree / ".git").write_text(f"gitdir: {pointer}\n", encoding="utf-8")

    common = _git_common_dir(worktree, tmp_path / "repositories")

    assert common == repository / ".git"


def test_git_common_dir_accepts_relative_pointer(tmp_path: Path) -> None:
    repository, worktree = _make_worktree_fixture(tmp_path)
    pointer = repository / ".git" / "worktrees" / "abc"
    relative = pointer.relative_to(worktree, walk_up=True)
    (worktree / ".git").write_text(f"gitdir: {relative}\n", encoding="utf-8")

    assert _git_common_dir(worktree, tmp_path / "repositories") == repository / ".git"


def test_git_common_dir_accepts_plain_repository(tmp_path: Path) -> None:
    repository = tmp_path / "repositories" / "demo"
    (repository / ".git").mkdir(parents=True)

    assert _git_common_dir(repository, tmp_path / "repositories") == repository / ".git"


def test_git_common_dir_rejects_pointer_escaping_repository_root(tmp_path: Path) -> None:
    _, worktree = _make_worktree_fixture(tmp_path)
    outside = tmp_path / "elsewhere" / ".git" / "worktrees" / "abc"
    outside.mkdir(parents=True)
    (worktree / ".git").write_text(f"gitdir: {outside}\n", encoding="utf-8")

    with pytest.raises(PreviewMaterializationError, match="escapes"):
        _git_common_dir(worktree, tmp_path / "repositories")


@pytest.mark.parametrize("pointer", ["", "not-a-pointer", "gitdir:\n"])
def test_git_common_dir_rejects_malformed_pointer(tmp_path: Path, pointer: str) -> None:
    _, worktree = _make_worktree_fixture(tmp_path)
    (worktree / ".git").write_text(pointer, encoding="utf-8")

    with pytest.raises(PreviewMaterializationError):
        _git_common_dir(worktree, tmp_path / "repositories")


def test_git_common_dir_rejects_missing_pointer(tmp_path: Path) -> None:
    _, worktree = _make_worktree_fixture(tmp_path)

    with pytest.raises(PreviewMaterializationError, match="unreadable"):
        _git_common_dir(worktree, tmp_path / "repositories")


def test_export_commit_rejects_oversized_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "out"
    payload = _tar_bytes({"big.bin": b"x" * 64})
    monkey = FakeArchiveProcess(payload)

    import vuzol.workflows.runtime_preview as runtime_module

    monkeypatch.setattr(runtime_module, "_start_git_archive", lambda *_args: monkey)
    with pytest.raises(PreviewExportTooLarge):
        _export_commit(
            tmp_path,
            tmp_path,
            "a" * 40,
            destination,
            max_bytes=8,
            max_files=100,
        )

    assert monkey.killed


def test_cleanup_orphaned_runtimes_removes_leftover_state(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".runtime"
    (runtime_root / "runs" / "demo" / "token").mkdir(parents=True)
    (runtime_root / "staging" / "demo-token").mkdir(parents=True)

    cleanup_orphaned_runtimes(runtime_root)

    assert not (runtime_root / "runs").exists()
    assert not (runtime_root / "staging").exists()
    cleanup_orphaned_runtimes(runtime_root)


@pytest.mark.skipif(
    landlock.landlock_abi_version() < 1, reason="kernel Landlock support is required"
)
def test_landlock_wrapper_confines_child_writes(tmp_path: Path) -> None:
    import subprocess

    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys\n"
        "inside, outside = sys.argv[1], sys.argv[2]\n"
        "open(inside, 'w', encoding='utf-8').write('ok')\n"
        "try:\n"
        "    open(outside, 'w', encoding='utf-8').write('bad')\n"
        "except PermissionError:\n"
        "    sys.exit(0)\n"
        "sys.exit(2)\n",
        encoding="utf-8",
    )
    wrapper = Path(landlock.__file__) if landlock.__file__ else Path()
    spec = json.dumps({"read_only": [str(tmp_path)], "read_write": [str(runtime_dir)]})

    completed = subprocess.run(
        (
            sys.executable,
            str(wrapper),
            spec,
            "--",
            sys.executable,
            str(probe),
            str(runtime_dir / "inside.txt"),
            str(tmp_path / "outside.txt"),
        ),
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode()
    assert (runtime_dir / "inside.txt").read_text(encoding="utf-8") == "ok"
    assert not (tmp_path / "outside.txt").exists()


@pytest.mark.parametrize(
    "arguments",
    [
        (),
        ("{}",),
        ("{}", "--"),
        ("not-json", "--", "/bin/true"),
        ('{"read_write": []}', "--", "/bin/true"),
        ('{"read_write": ["/tmp"]}', "no-separator", "/bin/true"),
        ('{"read_write": ["/tmp"]}', "--"),
        ('{"read_write": ["/tmp"]}', "--", "relative-command"),
    ],
)
def test_landlock_wrapper_rejects_malformed_invocations(arguments: tuple[str, ...]) -> None:
    import subprocess

    wrapper = Path(landlock.__file__) if landlock.__file__ else Path()

    completed = subprocess.run(
        (sys.executable, str(wrapper), *arguments),
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == landlock.EXIT_BAD_SPEC


def test_landlock_abi_version_is_not_negative() -> None:
    assert landlock.landlock_abi_version() >= 0


@pytest.mark.parametrize(
    ("abi_version", "expected"),
    [
        (1, (1 << 13) - 1),
        (2, (1 << 14) - 1),
        (3, (1 << 15) - 1),
        (4, (1 << 15) - 1),
        (5, (1 << 16) - 1),
    ],
)
def test_landlock_supported_fs_access_matches_abi(abi_version: int, expected: int) -> None:
    assert landlock._supported_fs_access(abi_version) == expected


def test_landlock_apply_uses_compatible_ruleset_size_and_rights(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    read_only = tmp_path / "read-only"
    read_write = tmp_path / "read-write"
    device = tmp_path / "device"
    read_only.mkdir()
    read_write.mkdir()
    device.write_text("device", encoding="utf-8")
    calls: list[tuple[int, tuple[object, ...]]] = []

    def syscall(number: int, *arguments: object) -> int:
        calls.append((number, arguments))
        if number == landlock._LANDLOCK_CREATE_RULESET:
            return os.open(os.devnull, os.O_RDONLY)
        return 0

    fake = SimpleNamespace(syscall=syscall, prctl=MagicMock(return_value=0))
    monkeypatch.setattr(landlock, "_load_libc", lambda: fake)
    monkeypatch.setattr(landlock, "landlock_abi_version", lambda: 1)

    landlock.apply_confinement(
        (str(read_only),),
        (str(read_write),),
        ((str(device), landlock.ACCESS_FS_READ_FILE | landlock.ACCESS_FS_IOCTL_DEV),),
    )

    create_call = next(
        arguments for number, arguments in calls if number == landlock._LANDLOCK_CREATE_RULESET
    )
    assert create_call[1] == ctypes.sizeof(ctypes.c_uint64)
    assert cast(Any, create_call[0])._obj.handled_access_fs == (1 << 13) - 1
    add_calls = [arguments for number, arguments in calls if number == landlock._LANDLOCK_ADD_RULE]
    assert cast(Any, add_calls[0][2])._obj.allowed_access == landlock._READ_ONLY_ACCESS & (
        (1 << 13) - 1
    )
    assert cast(Any, add_calls[1][2])._obj.allowed_access == landlock.ACCESS_FS_READ_FILE
    assert cast(Any, add_calls[2][2])._obj.allowed_access == (1 << 13) - 1


@pytest.mark.parametrize(
    "arguments",
    [
        (),
        ("not-json", "--", "/bin/true"),
        ('{"read_write": []}', "--", "/bin/true"),
        ('{"read_write": 5}', "--", "/bin/true"),
        ('{"read_only": 5, "read_write": ["/tmp"]}', "--", "/bin/true"),
        ('{"read_write": [""]}', "--", "/bin/true"),
        ('{"read_write": ["/tmp"]}', "no-separator", "/bin/true"),
        ('{"read_write": ["/tmp"]}', "--"),
        ('{"read_write": ["/tmp"]}', "--", "relative-command"),
    ],
)
def test_landlock_main_rejects_malformed_invocations_in_process(arguments: tuple[str, ...]) -> None:
    assert landlock.main(arguments) == landlock.EXIT_BAD_SPEC


def test_landlock_main_fails_closed_when_rw_path_missing(tmp_path: Path) -> None:
    spec = json.dumps({"read_write": [str(tmp_path / "missing")]})

    assert landlock.main((spec, "--", "/bin/true")) == landlock.EXIT_CONFINEMENT_FAILED


def test_landlock_apply_rejects_missing_read_only_path(tmp_path: Path) -> None:
    with pytest.raises(landlock.LandlockUnavailable, match="not openable"):
        landlock.apply_confinement((str(tmp_path / "missing"),), ())


def test_landlock_apply_rejects_missing_read_write_path(tmp_path: Path) -> None:
    (tmp_path / "ro").mkdir()

    with pytest.raises(landlock.LandlockUnavailable, match="not openable"):
        landlock.apply_confinement((str(tmp_path / "ro"),), (str(tmp_path / "missing"),))


def test_landlock_apply_reports_ruleset_creation_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = SimpleNamespace(syscall=MagicMock(return_value=-1))
    monkeypatch.setattr(landlock, "_load_libc", lambda: fake)
    monkeypatch.setattr(landlock, "landlock_abi_version", lambda: 1)

    with pytest.raises(landlock.LandlockUnavailable, match="landlock_create_ruleset"):
        landlock.apply_confinement((), (str(tmp_path),))


def test_landlock_abi_version_returns_zero_without_libc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken() -> object:
        raise OSError("libc unavailable")

    monkeypatch.setattr(landlock, "_load_libc", broken)

    assert landlock.landlock_abi_version() == 0


def test_landlock_abi_version_returns_zero_on_failed_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = SimpleNamespace(syscall=MagicMock(return_value=-1))
    monkeypatch.setattr(landlock, "_load_libc", lambda: fake)

    assert landlock.landlock_abi_version() == 0


def test_landlock_interpreter_defaults_point_at_existing_paths() -> None:
    defaults = landlock.interpreter_read_only()

    assert defaults
    for path, access in defaults:
        assert os.path.exists(path)
        assert access > 0
