import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.runtime_preview import (
    PreviewRuntimeRegistry,
    RuntimePreviewHandler,
    RuntimeTarget,
    _free_loopback_port,
    _wait_until_ready,
    _web_component,
)


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _request() -> StepExecutionRequest:
    return cast(
        StepExecutionRequest,
        SimpleNamespace(task_id="task", run_id="run"),
    )


def _handler(tmp_path: Path) -> tuple[RuntimePreviewHandler, MagicMock, MagicMock]:
    read_session = MagicMock()
    write_session = MagicMock()
    factory = MagicMock(return_value=AsyncContext(read_session))
    factory.begin.return_value = AsyncContext(write_session)
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            preview_site_root=tmp_path / "previews",
            preview_site_base_url="https://test.example",
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
    process = SimpleNamespace(
        returncode=None,
        terminate=MagicMock(),
        kill=MagicMock(),
        wait=AsyncMock(),
    )
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "published"
    assert outcome.result["public_url"] == "https://test.example/falling-worlds/"
    assert write_session.get.return_value.preview_url == outcome.result["public_url"]
    assert handler._registry.targets["falling-worlds"].port == 43210
    assert spawn.await_args is not None
    kwargs = spawn.await_args.kwargs
    assert kwargs["env"]["HOST"] == "127.0.0.1"
    assert kwargs["env"]["PORT"] == "43210"
    assert kwargs["cwd"] == str(tmp_path)
    await handler._registry.close()
    process.terminate.assert_called_once()


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

    first.terminate.assert_called_once()
    await registry.close()
    second.terminate.assert_called_once()
    assert registry.targets == {}


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
    process = SimpleNamespace(returncode=None, terminate=MagicMock(), wait=AsyncMock())
    monkeypatch.setattr(asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    monkeypatch.setattr(
        runtime_module,
        "_wait_until_ready",
        AsyncMock(side_effect=TimeoutError("not ready")),
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "preview_runtime_unhealthy"
    assert outcome.result["log_path"].endswith("demo.log")
    process.terminate.assert_called_once()
    process.wait.assert_awaited_once()


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
