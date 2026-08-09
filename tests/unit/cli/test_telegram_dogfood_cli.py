import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.cli import telegram_dogfood
from vuzol.cli.telegram_dogfood import _parse_args, _run
from vuzol.config import TopicKind
from vuzol.ops.telegram_dogfood import DogfoodFault


def test_dogfood_cli_has_explicit_session_report_diagnostic_and_fault_commands() -> None:
    session_id = uuid.uuid4()
    assert _parse_args(["start", "--project", "vuzol-test"]).project == "vuzol-test"
    assert _parse_args(["report", "--session", str(session_id)]).session == session_id
    assert _parse_args(["diagnose", "--package", str(session_id)]).package == session_id
    fault = _parse_args(
        [
            "arm-fault",
            "--session",
            str(session_id),
            "--project",
            "vuzol-test",
            "--fault",
            DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS.value,
        ]
    )
    assert fault.fault == DogfoodFault.PROVIDER_TIMEOUT_BEFORE_EFFECTS.value


def test_dogfood_cli_rejects_unknown_fault() -> None:
    with pytest.raises(SystemExit):
        _parse_args(
            [
                "arm-fault",
                "--session",
                str(uuid.uuid4()),
                "--project",
                "vuzol-test",
                "--fault",
                "run-arbitrary-command",
            ]
        )


@pytest.mark.anyio
@pytest.mark.parametrize("command", ["start", "arm-fault", "diagnose", "report"])
async def test_dogfood_cli_executes_each_operator_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    entity_id = uuid.uuid4()
    engine = SimpleNamespace(dispose=AsyncMock())
    session = MagicMock()

    @asynccontextmanager
    async def context():
        yield session

    factory = MagicMock()
    factory.begin.side_effect = context
    factory.side_effect = context
    project = SimpleNamespace(
        id="vuzol-test", repository_path=SimpleNamespace(), default_branch="main"
    )
    projects = SimpleNamespace(get=lambda _project_id: project)
    topic = SimpleNamespace(kind=TopicKind.PROJECT, project_id="vuzol-test", enabled=True)
    runtime = SimpleNamespace(
        settings=SimpleNamespace(telegram_dogfood=SimpleNamespace()),
        registries=SimpleNamespace(
            projects=projects,
            topics=SimpleNamespace(items=lambda: (topic,)),
            revision="r" * 64,
        ),
    )
    monkeypatch.setattr(telegram_dogfood, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(telegram_dogfood, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(telegram_dogfood, "resolve_database_dsn", lambda _settings: "dsn")
    monkeypatch.setattr(telegram_dogfood, "create_session_factory", lambda _engine: factory)
    monkeypatch.setattr(telegram_dogfood, "require_migration_head", AsyncMock())
    monkeypatch.setattr(
        telegram_dogfood.LocalGit, "resolve_commit", AsyncMock(return_value="a" * 40)
    )
    monkeypatch.setattr(telegram_dogfood, "start_session", AsyncMock(return_value=entity_id))
    monkeypatch.setattr(telegram_dogfood, "arm_fault", AsyncMock(return_value=entity_id))
    report = SimpleNamespace(to_dict=lambda: {"session_id": str(entity_id)})
    monkeypatch.setattr(telegram_dogfood, "build_report", AsyncMock(return_value=report))
    monkeypatch.setattr(telegram_dogfood, "diagnose_package", AsyncMock(return_value=report))
    args = {
        "start": ["start", "--project", "vuzol-test"],
        "arm-fault": [
            "arm-fault",
            "--session",
            str(entity_id),
            "--project",
            "vuzol-test",
            "--fault",
            DogfoodFault.PROVIDER_QUOTA_BEFORE_EFFECTS.value,
        ],
        "diagnose": ["diagnose", "--package", str(entity_id)],
        "report": ["report", "--session", str(entity_id)],
    }[command]

    assert await _run(_parse_args(args)) == 0
    assert str(entity_id) in capsys.readouterr().out
    engine.dispose.assert_awaited_once()


@pytest.mark.anyio
async def test_dogfood_start_rejects_project_without_one_topic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    engine = SimpleNamespace(dispose=AsyncMock())
    runtime = SimpleNamespace(
        settings=SimpleNamespace(),
        registries=SimpleNamespace(
            projects=SimpleNamespace(
                get=lambda _project_id: SimpleNamespace(
                    id="vuzol-test", repository_path=tmp_path, default_branch="main"
                )
            ),
            topics=SimpleNamespace(items=lambda: ()),
        ),
    )
    monkeypatch.setattr(telegram_dogfood, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(telegram_dogfood, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(telegram_dogfood, "resolve_database_dsn", lambda _settings: "dsn")
    monkeypatch.setattr(telegram_dogfood, "require_migration_head", AsyncMock())

    with pytest.raises(ValueError, match="exactly one"):
        await _run(_parse_args(["start", "--project", "vuzol-test"]))
    engine.dispose.assert_awaited_once()


def test_dogfood_main_exits_with_command_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telegram_dogfood, "_run", AsyncMock(return_value=0))
    with pytest.raises(SystemExit) as exit_info:
        telegram_dogfood.main(["start", "--project", "vuzol-test"])
    assert exit_info.value.code == 0
