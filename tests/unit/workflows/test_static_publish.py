"""Workflow publication adapter tests."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vuzol.config import RuntimeConfiguration, StaticDeploymentConfig
from vuzol.ops.static_publish import measure_static_tree
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.static_publish import StaticPublishHandler, _probe_public_site


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
            worktree_root=repositories,
            static_site_root=sites,
            static_site_base_url="https://hryshyn.dev/",
        ),
        registries=SimpleNamespace(projects=SimpleNamespace(get=lambda _project_id: project)),
    )
    session = AsyncMock()
    result_commit = "a" * 40
    worktree = SimpleNamespace(path=str(repository), result_commit=result_commit)
    build_result = {
        "status": "built",
        "source_commit": result_commit,
        "source_directory": ".",
        "artifact_hash": (measure_static_tree(repository).digest if with_entrypoint else "b" * 64),
    }
    build_step = SimpleNamespace(result=build_result)
    session.get.return_value = SimpleNamespace(project_id="demo")
    session.scalar.side_effect = [worktree, build_step, None]
    context = AsyncMock()
    context.__aenter__.return_value = session
    factory = MagicMock(return_value=context)
    request = SimpleNamespace(task_id=uuid4(), run_id=uuid4())
    probe = AsyncMock(
        return_value={
            "status_code": 200,
            "content_type": "text/html",
            "bytes": 13,
            "final_url": "https://hryshyn.dev/demo/",
        }
    )
    return StaticPublishHandler(factory, cast(RuntimeConfiguration, runtime), probe=probe), request


@pytest.mark.anyio
async def test_handler_publishes_and_returns_public_url(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "published"
    assert outcome.result["public_url"] == "https://hryshyn.dev/demo/"
    assert outcome.result["probe"]["status_code"] == 200
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


@pytest.mark.anyio
async def test_handler_blocks_when_public_probe_fails(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    handler._probe = AsyncMock(side_effect=ValueError("published site returned HTTP 404"))
    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "static_publish_probe_failed"


@pytest.mark.anyio
async def test_handler_requires_build_evidence(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    factory = cast(MagicMock, handler._factory)
    session = factory.return_value.__aenter__.return_value
    session.scalar.side_effect = [None, None, None]

    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "static_build_evidence_missing"


@pytest.mark.anyio
async def test_handler_rolls_back_hash_mismatch(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    factory = cast(MagicMock, handler._factory)
    session = factory.return_value.__aenter__.return_value
    worktree, build_step, materialization = session.scalar.side_effect
    build_step.result["artifact_hash"] = "f" * 64
    session.scalar.side_effect = [worktree, build_step, materialization]
    rollback = AsyncMock()
    handler_mock = cast(object, handler)
    cast(Any, handler_mock)._rollback_changed = rollback

    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]

    assert outcome.category == "static_build_hash_mismatch"
    rollback.assert_awaited_once()


@pytest.mark.anyio
async def test_handler_honors_cancellation(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    cancellation = CancellationContext()
    cancellation.request()

    outcome = await handler.execute(request, cancellation)  # type: ignore[arg-type]

    assert outcome.kind is OutcomeKind.CANCELLED


@pytest.mark.anyio
async def test_handler_skips_missing_project(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    factory = cast(MagicMock, handler._factory)
    session = factory.return_value.__aenter__.return_value
    session.get.return_value = None

    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]

    assert outcome.result == {"status": "skipped", "reason": "project_missing"}


@pytest.mark.anyio
async def test_handler_does_not_publish_intermediate_package_item(tmp_path: Path) -> None:
    handler, request = _handler(tmp_path)
    factory = cast(MagicMock, handler._factory)
    session = factory.return_value.__aenter__.return_value
    worktree, build_step, _materialization = session.scalar.side_effect
    session.scalar.side_effect = [
        worktree,
        build_step,
        SimpleNamespace(ordinal=2, plan_revision_id=uuid4()),
        4,
    ]

    outcome = await handler.execute(request, CancellationContext())  # type: ignore[arg-type]

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result == {
        "status": "skipped",
        "reason": "package_intermediate_item",
        "ordinal": 2,
        "plan_size": 4,
    }
    handler._probe.assert_not_awaited()  # type: ignore[union-attr]


@pytest.mark.anyio
async def test_public_probe_returns_bounded_http_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    response = SimpleNamespace(
        status_code=200,
        content=b"<html></html>",
        headers={"content-type": "text/html"},
        url="https://example.test/demo/",
    )
    client = AsyncMock()
    client.get.return_value = response
    context = AsyncMock()
    context.__aenter__.return_value = client
    monkeypatch.setattr("vuzol.workflows.static_publish.httpx.AsyncClient", lambda **_kw: context)

    evidence = await _probe_public_site("https://example.test/demo/")

    assert evidence["status_code"] == 200
    assert evidence["bytes"] == len(response.content)
