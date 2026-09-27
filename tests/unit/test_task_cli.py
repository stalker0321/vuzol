"""Unit tests for the vuzol-task operator CLI (WP09)."""

import argparse
import uuid

import pytest

from vuzol.cli.task import _parse_args


def test_parse_commands_and_ids() -> None:
    task_id = str(uuid.uuid4())
    args = _parse_args(["pause", "--task-id", task_id, "--user-id", "7"])
    assert args.command == "pause"
    assert args.task_id == task_id
    assert args.user_id == 7
    assert args.expected_version is None
    args = _parse_args(
        ["inspect", "--task-id", task_id, "--user-id", "7", "--expected-version", "3"]
    )
    assert args.command == "inspect" and args.expected_version == 3


def test_parse_rejects_unknown_command() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["self-destruct", "--task-id", str(uuid.uuid4()), "--user-id", "7"])


def test_parse_requires_task_and_user() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["pause", "--user-id", "7"])
    with pytest.raises(SystemExit):
        _parse_args(["pause", "--task-id", str(uuid.uuid4())])


def _cli_args(command: str = "pause") -> argparse.Namespace:
    return _parse_args(
        [
            "pause" if command == "pause" else command,
            "--task-id",
            str(uuid.uuid4()),
            "--user-id",
            "7",
        ]
    )


def _fake_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    module = sys.modules["vuzol.cli.task"]
    settings = SimpleNamespace(service_name="vuzol", log_level="INFO")
    monkeypatch.setattr(
        module,
        "get_runtime_configuration",
        lambda **kwargs: SimpleNamespace(settings=settings),
    )
    monkeypatch.setattr(module, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(module, "get_logger", lambda name: MagicMock())
    monkeypatch.setattr(module, "resolve_database_dsn", lambda *args, **kwargs: "dsn")
    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(module, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(module, "create_session_factory", lambda engine: MagicMock())


def test_unknown_task_maps_to_exit_code_3(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    import vuzol.cli.task as task_cli

    _fake_runtime(monkeypatch)

    class _MissingService:
        def __init__(self, factory: object) -> None:
            pass

        async def execute(self, **kwargs: object) -> object:
            raise ValueError("task not found: deadbeef")

    monkeypatch.setattr(task_cli, "TaskControlService", _MissingService)
    assert asyncio.run(task_cli._run(_cli_args())) == 3


def test_applied_command_maps_to_exit_code_0(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio
    import uuid

    import vuzol.cli.task as task_cli
    from vuzol.workflows.application import TaskCommandResult

    _fake_runtime(monkeypatch)
    task_id = uuid.uuid4()

    class _OkService:
        def __init__(self, factory: object) -> None:
            pass

        async def execute(self, **kwargs: object) -> TaskCommandResult:
            return TaskCommandResult(task_id=task_id, version=2, status="paused", applied=True)

    monkeypatch.setattr(task_cli, "TaskControlService", _OkService)
    assert asyncio.run(task_cli._run(_cli_args())) == 0
