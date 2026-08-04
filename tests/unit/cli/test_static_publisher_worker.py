import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.anyio
async def test_static_publisher_worker_processes_then_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vuzol.cli.static_publisher_worker as cli
    from vuzol.config import Capability
    from vuzol.storage.types import QueueClass

    settings = SimpleNamespace(
        service_name="vuzol",
        log_level="INFO",
        workflow=SimpleNamespace(poll_interval_seconds=0.01),
    )
    runtime = SimpleNamespace(settings=settings)
    engine = MagicMock()
    engine.dispose = AsyncMock()
    factory = MagicMock()
    worker = MagicMock()
    worker.process_one = AsyncMock(side_effect=[True, False])
    stop = MagicMock()
    stop.is_set.side_effect = [False, False, True]
    stop.wait = AsyncMock()
    loop = MagicMock()

    monkeypatch.setattr(cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(cli, "configure_logging", MagicMock())
    monkeypatch.setattr(cli, "resolve_database_dsn", lambda _settings: "dsn")
    monkeypatch.setattr(cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(cli, "require_migration_head", AsyncMock())
    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: factory)
    handler = object()
    monkeypatch.setattr(cli, "StaticPublishHandler", lambda *_args: handler)
    worker_factory = MagicMock(return_value=worker)
    monkeypatch.setattr(cli, "WorkflowWorker", worker_factory)
    monkeypatch.setattr(asyncio, "Event", lambda: stop)
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)

    async def timeout(coro: object, *, timeout: float) -> None:  # noqa: ASYNC109
        del timeout
        await coro  # type: ignore[misc]
        raise TimeoutError

    wait_for = AsyncMock(side_effect=timeout)
    monkeypatch.setattr(asyncio, "wait_for", wait_for)

    await cli.run()

    kwargs = worker_factory.call_args.kwargs
    assert kwargs["handlers"] == {"publish_static": handler}
    assert kwargs["capabilities"] == frozenset({Capability.FILESYSTEM_WRITE})
    assert kwargs["queue_classes"] == frozenset({QueueClass.LIGHT})
    assert worker.process_one.await_count == 2
    wait_for.assert_awaited_once()
    assert loop.add_signal_handler.call_count == 2
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_static_publisher_worker_disposes_engine_on_preflight_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import vuzol.cli.static_publisher_worker as cli

    settings = SimpleNamespace(service_name="vuzol", log_level="INFO")
    runtime = SimpleNamespace(settings=settings)
    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(cli, "configure_logging", MagicMock())
    monkeypatch.setattr(cli, "resolve_database_dsn", lambda _settings: "dsn")
    monkeypatch.setattr(cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(cli, "require_migration_head", AsyncMock(side_effect=RuntimeError("old")))

    with pytest.raises(RuntimeError, match="old"):
        await cli.run()
    engine.dispose.assert_awaited_once()
