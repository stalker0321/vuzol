"""Managed HTTP preview runtime for approved project environment contracts.

Preview processes never execute inside the retained task worktree. The
approved ``result_commit`` is exported into a disposable per-run runtime
directory, and the runtime process is confined with Landlock so that only
that directory is writable (ADR-0010). Project code is untrusted input: the
confinement fails closed instead of degrading to an unconstrained spawn.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import socket
import subprocess  # fixed argv only, no shell
import sys
import tarfile
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.project_environment import current_environment
from vuzol.projects.capabilities import CapabilityState, preflight_capabilities
from vuzol.security import landlock
from vuzol.storage.models import MaterializationLink, Task, WorkPackage, Worktree
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

_PROJECT_ID = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_GITDIR_POINTER = re.compile(r"gitdir:\s*(.+?)\s*\Z", re.DOTALL)
_SUPPORTED_EXECUTABLES = {"node": "/usr/bin/node"}


class PreviewMaterializationError(RuntimeError):
    """The result commit could not be exported into the runtime directory."""


class PreviewExportTooLarge(RuntimeError):
    """The exported archive exceeded the configured byte or file bound."""


@dataclass(slots=True)
class RuntimeTarget:
    project_id: str
    port: int
    process: asyncio.subprocess.Process
    source_commit: str
    runtime_dir: Path | None = None


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    with suppress(TimeoutError):
        await asyncio.wait_for(process.wait(), timeout=5)
    if process.returncode is None:
        process.kill()
        await process.wait()


def _remove_runtime_dir(runtime_dir: Path | None) -> None:
    if runtime_dir is not None:
        shutil.rmtree(runtime_dir, ignore_errors=True)


@dataclass(slots=True)
class PreviewRuntimeRegistry:
    targets: dict[str, RuntimeTarget] = field(default_factory=dict)

    async def replace(self, target: RuntimeTarget) -> None:
        previous = self.targets.get(target.project_id)
        self.targets[target.project_id] = target
        if previous is not None:
            await _stop_process(previous.process)
            _remove_runtime_dir(previous.runtime_dir)

    async def close(self) -> None:
        targets = tuple(self.targets.values())
        self.targets.clear()
        for target in targets:
            await _stop_process(target.process)
            _remove_runtime_dir(target.runtime_dir)


def cleanup_orphaned_runtimes(runtime_root: Path) -> None:
    """Remove per-run runtime state left behind by a previous publisher run."""
    for name in ("runs", "staging"):
        shutil.rmtree(runtime_root / name, ignore_errors=True)


def _git_common_dir(worktree_path: Path, repository_root: Path) -> Path:
    dot_git = worktree_path / ".git"
    if dot_git.is_dir():
        return dot_git
    try:
        pointer = dot_git.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        raise PreviewMaterializationError("worktree .git pointer is unreadable") from error
    match = _GITDIR_POINTER.fullmatch(pointer.strip())
    if match is None:
        raise PreviewMaterializationError("worktree .git pointer is malformed")
    candidate = Path(match.group(1))
    if not candidate.is_absolute():
        candidate = worktree_path / candidate
    resolved = candidate.resolve()
    root = repository_root.resolve()
    if resolved != root and root not in resolved.parents:
        raise PreviewMaterializationError("worktree git directory escapes the repository root")
    common = resolved.parent.parent
    if common.name != ".git" or not common.is_dir():
        raise PreviewMaterializationError("worktree git common directory not found")
    return common


def _start_git_archive(
    worktree_path: Path, common_dir: Path, commit: str
) -> subprocess.Popen[bytes]:
    argv = (
        "git",
        "-C",
        str(worktree_path),
        "-c",
        f"safe.directory={worktree_path}",
        "-c",
        f"safe.directory={common_dir}",
        "archive",
        "--format=tar",
        commit,
    )
    return subprocess.Popen(  # noqa: S603 - fixed argv assembled from validated values
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _export_commit(
    worktree_path: Path,
    common_dir: Path,
    commit: str,
    destination: Path,
    *,
    max_bytes: int,
    max_files: int,
) -> None:
    destination.mkdir(parents=True)
    process = _start_git_archive(worktree_path, common_dir, commit)
    stdout = process.stdout
    stderr = process.stderr
    assert stdout is not None and stderr is not None
    total_bytes = 0
    total_files = 0
    try:
        with tarfile.open(fileobj=stdout, mode="r|") as archive:
            for member in archive:
                total_files += 1
                if total_files > max_files:
                    raise PreviewExportTooLarge(f"archive exceeds {max_files} files")
                total_bytes += max(member.size, 0)
                if total_bytes > max_bytes:
                    raise PreviewExportTooLarge(f"archive exceeds {max_bytes} bytes")
                try:
                    archive.extract(member, destination, filter="data")
                except (tarfile.TarError, OSError) as error:
                    raise PreviewMaterializationError(
                        f"unsafe archive member {member.name!r}"
                    ) from error
        stdout.close()
        status = process.wait(timeout=60)
    except (PreviewExportTooLarge, PreviewMaterializationError):
        process.kill()
        with suppress(OSError):
            process.wait(timeout=10)
        raise
    except OSError as error:
        process.kill()
        with suppress(OSError):
            process.wait(timeout=10)
        raise PreviewMaterializationError(f"archive extraction failed: {error}") from error
    if status != 0:
        detail = stderr.read(2048).decode("utf-8", "replace").strip()
        raise PreviewMaterializationError(f"git archive exited with {status}: {detail}")


def _confinement_spec(read_only: Path, read_write: Path) -> str:
    return json.dumps({"read_only": [str(read_only)], "read_write": [str(read_write)]})


class RuntimePreviewHandler:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        runtime: RuntimeConfiguration,
        registry: PreviewRuntimeRegistry,
    ) -> None:
        self._factory = session_factory
        self._runtime = runtime
        self._registry = registry

    async def execute(
        self, request: StepExecutionRequest, cancellation: CancellationContext
    ) -> StepOutcome:
        if cancellation.requested:
            return StepOutcome(kind=OutcomeKind.CANCELLED, result={}, category="cancelled")
        async with self._factory() as session:
            task = await session.get(Task, request.task_id)
            worktree = await session.scalar(
                select(Worktree).where(
                    Worktree.task_id == request.task_id,
                    Worktree.run_id == request.run_id,
                )
            )
            materialization = await session.scalar(
                select(MaterializationLink).where(MaterializationLink.task_id == request.task_id)
            )
            environment = (
                None
                if task is None or task.project_id is None
                else await current_environment(session, task.project_id)
            )
        if task is None or task.project_id is None or worktree is None:
            return StepOutcome.succeeded({"status": "skipped", "reason": "project_missing"})
        component = _web_component(None if environment is None else environment.contract)
        if component is None:
            return StepOutcome.succeeded({"status": "skipped", "reason": "no_web_component"})
        contract = {} if environment is None else environment.contract
        blocked = tuple(
            check
            for check in preflight_capabilities(
                contract,
                managed_toolchain_root=(
                    self._runtime.settings.capability_provisioning.toolchain_root
                ),
            )
            if check.state is not CapabilityState.READY
        )
        if blocked:
            labels = ", ".join(check.label for check in blocked[:5])
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={
                    "status": "needs_setup",
                    "capabilities": [
                        {"key": check.key, "state": check.state, "detail": check.detail}
                        for check in blocked
                    ],
                },
                category="environment_setup_required",
                summary=f"project environment needs setup: {labels}",
            )
        command = component.get("run_command")
        if (
            not isinstance(command, list)
            or not command
            or not all(
                isinstance(value, str) and value and "\x00" not in value for value in command
            )
        ):
            return _needs_setup("web runtime command is missing or invalid")
        executable = _SUPPORTED_EXECUTABLES.get(command[0])
        if executable is None or not Path(executable).is_file():  # noqa: ASYNC240
            return _needs_setup(f"runtime adapter is unavailable for {command[0]}")
        commit = worktree.result_commit
        if commit is None or _COMMIT.fullmatch(commit) is None:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="preview_source_missing",
                summary="runtime preview requires a retained result commit",
            )
        if landlock.landlock_abi_version() < 1:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="preview_confinement_unavailable",
                summary="kernel Landlock confinement is unavailable",
            )
        prepared = await self._prepare_runtime_dir(task.project_id, Path(worktree.path), commit)
        if isinstance(prepared, StepOutcome):
            return prepared
        run_dir, app_dir = prepared
        log_path = run_dir / "preview.log"
        log = log_path.open("ab", buffering=0)
        port = _free_loopback_port()
        environment_variables = {
            "PATH": "/usr/bin:/bin",
            "HOME": str(run_dir / "home"),
            "TMPDIR": str(run_dir / "tmp"),
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "NODE_ENV": "test",
        }
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(_wrapper_path()),
                _confinement_spec(_source_root(), run_dir),
                "--",
                executable,
                *command[1:],
                cwd=str(app_dir),
                env=environment_variables,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=log,
                stderr=log,
            )
        finally:
            log.close()
        healthcheck = component.get("healthcheck_path")
        path = healthcheck if isinstance(healthcheck, str) else "/"
        try:
            probe = await _wait_until_ready(process, port=port, path=path)
        except (RuntimeError, httpx.HTTPError, TimeoutError) as error:
            await _stop_process(process)
            category = (
                "preview_confinement_unavailable"
                if process.returncode in (landlock.EXIT_BAD_SPEC, landlock.EXIT_CONFINEMENT_FAILED)
                else "preview_runtime_unhealthy"
            )
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"log_path": str(log_path)},
                category=category,
                summary=str(error)[:500],
            )
        target = RuntimeTarget(task.project_id, port, process, commit, runtime_dir=run_dir)
        await self._registry.replace(target)
        public_url = (
            f"{str(self._runtime.settings.preview_site_base_url).rstrip('/')}/{task.project_id}/"
        )
        if materialization is not None:
            async with self._factory.begin() as session:
                package = await session.get(WorkPackage, materialization.work_package_id)
                if package is not None:
                    package.preview_url = public_url
        return StepOutcome.succeeded(
            {
                "status": "published",
                "artifact_kind": "web_preview",
                "public_url": public_url,
                "source_commit": commit,
                "healthcheck": probe,
            }
        )

    async def _prepare_runtime_dir(
        self, project_id: str, worktree_path: Path, commit: str
    ) -> tuple[Path, Path] | StepOutcome:
        settings = self._runtime.settings
        runtime_root = settings.preview_site_root / ".runtime"
        token = uuid.uuid4().hex
        run_dir = runtime_root / "runs" / project_id / token
        staging_dir = runtime_root / "staging" / f"{project_id}-{token}"
        try:
            common_dir = _git_common_dir(worktree_path, settings.repository_root)
            await asyncio.to_thread(
                _export_commit,
                worktree_path,
                common_dir,
                commit,
                staging_dir,
                max_bytes=settings.preview_export_max_bytes,
                max_files=settings.preview_export_max_files,
            )
            (run_dir / "home").mkdir(parents=True)
            (run_dir / "tmp").mkdir()
            staging_dir.rename(run_dir / "app")
        except PreviewExportTooLarge as error:
            shutil.rmtree(staging_dir, ignore_errors=True)
            shutil.rmtree(run_dir, ignore_errors=True)
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="preview_export_too_large",
                summary=str(error)[:500],
            )
        except (PreviewMaterializationError, OSError) as error:
            shutil.rmtree(staging_dir, ignore_errors=True)
            shutil.rmtree(run_dir, ignore_errors=True)
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="preview_materialization_failed",
                summary=str(error)[:500],
            )
        return run_dir, run_dir / "app"


def _wrapper_path() -> Path:
    location = getattr(landlock, "__file__", None)
    if location is None:  # pragma: no cover - packaged layouts always expose __file__
        raise PreviewMaterializationError("landlock confinement wrapper is unavailable")
    return Path(location)


def _source_root() -> Path:
    return _wrapper_path().resolve().parents[2]


def _web_component(contract: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(contract, dict):
        return None
    components = contract.get("components")
    if not isinstance(components, dict):
        return None
    for component in components.values():
        if isinstance(component, dict) and component.get("kind") == "web_service":
            return component
    return None


def _needs_setup(summary: str) -> StepOutcome:
    return StepOutcome(
        kind=OutcomeKind.BLOCKED,
        result={"status": "needs_setup", "capability": "http-runtime"},
        category="environment_setup_required",
        summary=summary,
    )


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


async def _wait_until_ready(
    process: asyncio.subprocess.Process, *, port: int, path: str
) -> dict[str, object]:
    url = f"http://127.0.0.1:{port}{path}"
    async with httpx.AsyncClient(timeout=2.0) as client:
        for _attempt in range(30):
            if process.returncode is not None:
                raise RuntimeError(f"preview process exited with code {process.returncode}")
            try:
                response = await client.get(url)
                if 200 <= response.status_code < 500:
                    return {"status_code": response.status_code, "path": path}
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.25)
    raise TimeoutError("preview healthcheck timed out")
