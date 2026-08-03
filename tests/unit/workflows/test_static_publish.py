"""Workflow publication adapter tests."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vuzol.config import StaticDeploymentConfig
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.static_publish import StaticPublishHandler


def _handler(
    tmp_path: Path, *, with_entrypoint: bool = True
) -> tuple[StaticPublishHandler, object]:
    repositories = tmp_path / "repositories"
    sites = tmp_path / "sites"
    repository = repositories / "demo"
    repository.mkdir(parents=True)
    sites.mkdir()
    if with_entrypoint:
        (repository / "index.html").write_text("<h1>Demo</h1>")
    project = SimpleNamespace(
        id="demo",
        repository_path=repository,
        static_deployment=StaticDeploymentConfig(url_path="demo"),
    )
    runtime = SimpleNamespace(
        settings=SimpleNamespace(
            repository_root=repositories,
            static_site_root=sites,
            static_site_base_url="https://hryshyn.dev/",
        ),
        registries=SimpleNamespace(projects=SimpleNamespace(get=lambda _project_id: project)),
    )
    session = AsyncMock()
    session.get.return_value = SimpleNamespace(project_id="demo")
    context = AsyncMock()
    context.__aenter__.return_value = session
    factory = MagicMock(return_value=context)
    request = SimpleNamespace(task_id=uuid4())
    return StaticPublishHandler(factory, runtime), request


@pytest.mark.anyio
async def test_handler_publishes_and_returns_public_url(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "published"
    assert outcome.result["public_url"] == "https://hryshyn.dev/demo/"
    assert (tmp_path / "sites/demo/current/index.html").read_text() == "<h1>Demo</h1>"


@pytest.mark.anyio
async def test_handler_skips_project_without_entrypoint(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path, with_entrypoint=False)
    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result == {
        "status": "skipped",
        "reason": "entrypoint_missing",
        "public_url": "https://hryshyn.dev/demo/",
    }
