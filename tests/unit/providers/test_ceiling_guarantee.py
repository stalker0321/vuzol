"""Adapter ceiling statements stay narrower than a reserved estimate."""

from pathlib import Path

from tests.integration.providers._test_routing_helpers import profile
from vuzol.providers.control import ceiling_is_admission_only


def test_cli_and_tool_ceilings_are_admission_only(tmp_path: Path) -> None:
    cli = profile(
        "cli-worker",
        launch_mode="cli",
        runtime_identity="codex-a",
        state_directory=tmp_path / "codex-state",
    )
    tool = profile("tool-worker", launch_mode="tool")
    api = profile("api-worker")

    assert ceiling_is_admission_only(cli)
    assert ceiling_is_admission_only(tool)
    assert ceiling_is_admission_only(api) is False
