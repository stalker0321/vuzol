import hashlib
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.artifact_production import (
    ArtifactCommandResult,
    ArtifactProductionHandler,
    SandboxedArtifactCommandRunner,
    _matched_artifacts,
)
from vuzol.execution.codex import _artifact_argv
from vuzol.storage.models import Artifact
from vuzol.storage.types import StepStatus
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest


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
        SimpleNamespace(
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            step_id=uuid.uuid4(),
            lease=SimpleNamespace(generation=1),
        ),
    )


def _artifact(artifact_type: str, content: bytes) -> Artifact:
    return Artifact(
        artifact_type=artifact_type,
        content_uri="artifact:test",
        size_bytes=len(content),
        content_hash=hashlib.sha256(content).hexdigest(),
        media_type="application/json",
        sensitivity="internal",
        visibility="private",
    )


def _handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    contract: dict[str, object],
    run: ArtifactCommandResult | None = None,
    head: str = "a" * 40,
) -> tuple[ArtifactProductionHandler, Any, MagicMock, MagicMock]:
    read = MagicMock()
    read.get = AsyncMock(
        side_effect=[
            SimpleNamespace(project_id="demo"),
            SimpleNamespace(status=StepStatus.RUNNING, timeout_seconds=60),
        ]
    )
    worktree = SimpleNamespace(id=uuid.uuid4(), path=str(tmp_path), result_commit="a" * 40)
    read.scalar = AsyncMock(return_value=worktree)
    write = MagicMock()
    factory = MagicMock(return_value=AsyncContext(read))
    factory.begin.return_value = AsyncContext(write)
    monkeypatch.setattr(
        "vuzol.execution.artifact_production.current_environment",
        AsyncMock(return_value=SimpleNamespace(contract=contract)),
    )
    runner = MagicMock()
    runner.run = AsyncMock(
        return_value=run
        or ArtifactCommandResult(
            exit_code=0,
            stdout="ok\n",
            stderr="",
            duration_ms=12,
            validation_image_digest=f"validation@sha256:{'b' * 64}",
        )
    )
    store = MagicMock()
    registries = MagicMock()
    registries.projects.get.return_value = SimpleNamespace(validation_sandbox_profile="validation")
    registries.sandboxes.get.return_value = SimpleNamespace(uid=10001, gid=10001)
    git = MagicMock()
    git.resolve_commit = AsyncMock(return_value=head)
    git.require_clean_tracked_worktree = AsyncMock()
    lease = MagicMock()
    lease.revoke = AsyncMock()
    access = MagicMock()
    access.grant = AsyncMock(return_value=lease)

    async def persist(_session: object, **kwargs: Any) -> Artifact:
        return _artifact(str(kwargs["artifact_type"]), cast(bytes, kwargs["content"]))

    store.persist = AsyncMock(side_effect=persist)
    handler = ArtifactProductionHandler(
        cast(Any, factory),
        registries,
        git,
        access,
        runner,
        store,
        worktree_root=tmp_path,
    )
    return handler, runner, store, access


@pytest.mark.anyio
async def test_sandboxed_runner_returns_bounded_command_evidence() -> None:
    envelope = SimpleNamespace(sandbox=SimpleNamespace(image="validation@sha256:digest"))
    envelopes = MagicMock()
    envelopes.build_artifact = AsyncMock(return_value=envelope)
    runtime = MagicMock()
    runtime.run = AsyncMock(
        return_value=SimpleNamespace(exit_code=0, stdout="ok", stderr="", duration_ms=7)
    )
    runner = SandboxedArtifactCommandRunner(envelopes, runtime)
    request = _request()
    cancellation = CancellationContext()

    result = await runner.run(
        request,
        worktree_id=uuid.uuid4(),
        argv=("python", "-m", "demo"),
        timeout_seconds=30,
        cancellation=cancellation,
    )

    assert result == ArtifactCommandResult(0, "ok", "", 7, "validation@sha256:digest")
    runtime.run.assert_awaited_once_with(envelope, cancellation)


@pytest.mark.anyio
async def test_artifact_producer_cancels_before_loading(tmp_path: Path) -> None:
    handler = ArtifactProductionHandler(
        cast(Any, MagicMock()),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        worktree_root=tmp_path,
    )
    cancellation = CancellationContext()
    cancellation.request()

    outcome = await handler.execute(_request(), cancellation)

    assert outcome.kind is OutcomeKind.CANCELLED


@pytest.mark.anyio
async def test_web_only_contract_skips_non_web_producer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler, runner, _store, access = _handler(
        tmp_path,
        monkeypatch,
        contract={"components": {"web": {"kind": "static_site", "label": "Web"}}},
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.result == {"status": "skipped", "reason": "no_artifacts"}
    runner.run.assert_not_awaited()
    access.grant.assert_not_awaited()


@pytest.mark.anyio
async def test_artifact_producer_rejects_wrong_source_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler, runner, _store, access = _handler(
        tmp_path,
        monkeypatch,
        contract={
            "components": {
                "cli": {"kind": "cli", "label": "CLI", "run_command": ["python", "cli.py"]}
            }
        },
        head="b" * 40,
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.category == "artifact_production_invalid"
    assert "source HEAD" in (outcome.summary or "")
    runner.run.assert_not_awaited()
    access.grant.return_value.revoke.assert_awaited_once()


@pytest.mark.anyio
async def test_cli_producer_runs_approved_command_and_persists_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler, runner, store, access = _handler(
        tmp_path,
        monkeypatch,
        contract={
            "components": {
                "cli": {
                    "kind": "cli",
                    "label": "CLI",
                    "run_command": ["python", "-m", "demo", "--help"],
                }
            }
        },
    )
    request = _request()

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "produced"
    assert len(outcome.result["artifacts"]) == 2
    assert runner.run.await_args.kwargs["argv"] == ("python", "-m", "demo", "--help")
    assert store.persist.await_count == 2
    access.grant.assert_awaited_once()


@pytest.mark.anyio
async def test_library_producer_persists_matched_package_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "dist" / "demo.whl"
    package.parent.mkdir()
    package.write_bytes(b"wheel")
    handler, _runner, store, _access = _handler(
        tmp_path,
        monkeypatch,
        contract={
            "components": {
                "library": {
                    "kind": "library",
                    "label": "Library",
                    "run_command": ["python", "-m", "build"],
                    "artifact_patterns": ["dist/*.whl"],
                }
            }
        },
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert [call.kwargs["artifact_type"] for call in store.persist.await_args_list] == [
        "package_archive",
        "package_archive_evidence",
    ]
    assert store.persist.await_args_list[0].kwargs["content"] == b"wheel"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("component", "run", "category"),
    [
        (
            {"kind": "cli", "label": "CLI", "run_command": []},
            None,
            "artifact_producer_setup_required",
        ),
        (
            {"kind": "worker", "label": "Worker", "run_command": ["python", "worker.py"]},
            ArtifactCommandResult(1, "", "failed", 1, f"image@sha256:{'b' * 64}"),
            "artifact_command_failed",
        ),
        (
            {
                "kind": "android_app",
                "label": "Android",
                "run_command": ["./gradlew", "assembleDebug"],
                "artifact_patterns": ["**/*.apk"],
            },
            None,
            "artifact_file_missing",
        ),
    ],
)
async def test_artifact_producer_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    component: dict[str, object],
    run: ArtifactCommandResult | None,
    category: str,
) -> None:
    handler, _runner, _store, _access = _handler(
        tmp_path,
        monkeypatch,
        contract={"components": {"component": component}},
        run=run,
    )

    outcome = await handler.execute(_request(), CancellationContext())

    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == category


def test_artifact_command_and_paths_are_bounded(tmp_path: Path) -> None:
    assert _artifact_argv(("python", "-m", "build")) == (
        "/opt/vuzol-validation/bin/python",
        "-m",
        "build",
    )
    for command in ((), ("sh", "-c", "true"), ("python", "bad\x00arg")):
        with pytest.raises(ValueError):
            _artifact_argv(command)

    (tmp_path / "one.apk").write_bytes(b"apk")
    assert _matched_artifacts(tmp_path, ("*.apk",)) == (tmp_path / "one.apk",)
    with pytest.raises(ValueError):
        _matched_artifacts(tmp_path, ("../*.apk",))
    assert _matched_artifacts(tmp_path, ()) == ()
    for index in range(21):
        (tmp_path / f"artifact-{index}.apk").write_bytes(b"apk")
    with pytest.raises(ValueError, match="too many"):
        _matched_artifacts(tmp_path, ("*.apk",))
