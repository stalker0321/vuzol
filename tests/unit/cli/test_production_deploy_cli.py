from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from vuzol.cli.production_deploy import main
from vuzol.ops.production_deploy import DeploymentError, ProductionDeployer


def test_production_deploy_cli_requires_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "geteuid", lambda: 1000)

    with pytest.raises(SystemExit, match="must run as root"):
        main(["--sha", "a" * 40])


def test_production_deploy_cli_emits_attested_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    deploy = lambda _self, _sha: SimpleNamespace(  # noqa: E731
        previous_sha="a" * 40,
        deployed_sha="b" * 40,
        rolled_back=False,
    )
    monkeypatch.setattr(ProductionDeployer, "deploy", deploy)

    main(["--sha", "b" * 40, "--source", str(tmp_path)])

    output = capsys.readouterr().out
    assert '"deployed_sha": "bbbb' in output
    assert '"rolled_back": false' in output


def test_production_deploy_cli_returns_bounded_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "geteuid", lambda: 0)

    def fail(_self: object, _sha: str) -> None:
        raise DeploymentError("attestation failed")

    monkeypatch.setattr(ProductionDeployer, "deploy", fail)

    with pytest.raises(SystemExit, match="attestation failed"):
        main(["--sha", "b" * 40])
