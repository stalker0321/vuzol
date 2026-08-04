from __future__ import annotations

import argparse
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest


class _Engine:
    def __init__(self) -> None:
        self.dispose = AsyncMock()


class _Factory:
    def __init__(self) -> None:
        self.session = MagicMock()

    @asynccontextmanager
    async def begin(self):  # type: ignore[no-untyped-def]
        yield self.session

    @asynccontextmanager
    async def __call__(self):  # type: ignore[no-untyped-def]
        yield self.session


def _wire_runtime(monkeypatch: pytest.MonkeyPatch) -> tuple[_Engine, _Factory]:
    import vuzol.cli.experiment as cli

    runtime = SimpleNamespace(settings=object(), registries=object())
    engine = _Engine()
    factory = _Factory()
    monkeypatch.setattr(cli, "get_runtime_configuration", lambda **_kwargs: runtime)
    monkeypatch.setattr(cli, "resolve_database_dsn", lambda _settings: "dsn")
    monkeypatch.setattr(cli, "create_engine", lambda *_args: engine)
    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: factory)
    return engine, factory


@pytest.mark.anyio
async def test_experiment_run_seed_and_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import vuzol.cli.experiment as cli
    from vuzol.experiments.domain import ExperimentTelemetry
    from vuzol.experiments.service import TrialSeedRequest

    engine, _factory = _wire_runtime(monkeypatch)
    request_file = tmp_path / "request.json"
    request_file.write_text("{}")
    seed_request = object()
    monkeypatch.setattr(
        TrialSeedRequest, "model_validate_json", MagicMock(return_value=seed_request)
    )
    trial = SimpleNamespace(
        task_uuid=uuid4(),
        run_uuid=uuid4(),
        interpretation_uuid=uuid4(),
        capsule=SimpleNamespace(model_dump=lambda **_kwargs: {"experiment_id": "exp"}),
    )
    seed = AsyncMock(return_value=trial)
    monkeypatch.setattr(cli, "seed_trial", seed)
    await cli._run(argparse.Namespace(command="seed", request=request_file))
    assert json.loads(capsys.readouterr().out)["run_uuid"] == str(trial.run_uuid)
    seed.assert_awaited_once()

    telemetry_file = tmp_path / "telemetry.json"
    telemetry_file.write_text("{}")
    telemetry = object()
    monkeypatch.setattr(
        ExperimentTelemetry, "model_validate_json", MagicMock(return_value=telemetry)
    )
    event_id = uuid4()
    record = AsyncMock(return_value=event_id)
    monkeypatch.setattr(cli, "record_trial", record)
    await cli._run(argparse.Namespace(command="record", telemetry=telemetry_file))
    assert json.loads(capsys.readouterr().out) == {"event_id": str(event_id)}
    assert engine.dispose.await_count == 2


@pytest.mark.anyio
async def test_experiment_run_inspect_and_export(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import vuzol.cli.experiment as cli

    engine, _factory = _wire_runtime(monkeypatch)
    inspect = AsyncMock(return_value={"experiment_id": "exp", "runs": []})
    monkeypatch.setattr(cli, "_inspect", inspect)
    await cli._run(argparse.Namespace(command="inspect", experiment_id="exp", latest=True))
    assert json.loads(capsys.readouterr().out)["experiment_id"] == "exp"
    inspect.assert_awaited_once()

    monkeypatch.setattr(cli, "load_trials", AsyncMock(return_value=[]))
    monkeypatch.setattr(cli, "aggregate_trials", lambda _trials: {"count": 0})
    write_csv = MagicMock()
    monkeypatch.setattr(cli, "_write_csv", write_csv)
    json_path = tmp_path / "nested" / "trials.json"
    csv_path = tmp_path / "nested" / "trials.csv"
    await cli._run(
        argparse.Namespace(
            command="export",
            experiment_id="exp",
            json=json_path,
            csv=csv_path,
        )
    )
    payload = json.loads(json_path.read_text())
    assert payload["schema_version"] == "step09a-export.v1"
    assert payload["summary"] == {"count": 0}
    write_csv.assert_called_once_with(csv_path, [])
    assert json.loads(capsys.readouterr().out)["trials"] == 0
    assert engine.dispose.await_count == 2


def test_experiment_main_parses_and_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    import vuzol.cli.experiment as cli

    args = argparse.Namespace(command="inspect")
    monkeypatch.setattr(cli, "_parse_args", lambda: args)
    run = AsyncMock()
    monkeypatch.setattr(cli, "_run", run)
    cli.main()
    run.assert_awaited_once_with(args)
