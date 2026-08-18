"""Managed HTTP preview runtime for approved project environment contracts."""

from __future__ import annotations

import asyncio
import re
import socket
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RuntimeConfiguration
from vuzol.project_environment import current_environment
from vuzol.projects.capabilities import CapabilityState, preflight_capabilities
from vuzol.storage.models import MaterializationLink, Task, WorkPackage, Worktree
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest

_PROJECT_ID = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_SUPPORTED_EXECUTABLES = {"node": "/usr/bin/node"}


@dataclass(slots=True)
class RuntimeTarget:
    project_id: str
    port: int
    process: asyncio.subprocess.Process
    source_commit: str


@dataclass(slots=True)
class PreviewRuntimeRegistry:
    targets: dict[str, RuntimeTarget] = field(default_factory=dict)

    async def replace(self, target: RuntimeTarget) -> None:
        previous = self.targets.get(target.project_id)
        self.targets[target.project_id] = target
        if previous is not None and previous.process.returncode is None:
            previous.process.terminate()
            with suppress(TimeoutError):
                await asyncio.wait_for(previous.process.wait(), timeout=5)
            if previous.process.returncode is None:
                previous.process.kill()
                await previous.process.wait()

    async def close(self) -> None:
        targets = tuple(self.targets.values())
        self.targets.clear()
        for target in targets:
            if target.process.returncode is None:
                target.process.terminate()
        for target in targets:
            if target.process.returncode is None:
                with suppress(TimeoutError):
                    await asyncio.wait_for(target.process.wait(), timeout=5)
            if target.process.returncode is None:
                target.process.kill()
                await target.process.wait()


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
            for check in preflight_capabilities(contract)
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
        if worktree.result_commit is None:
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={},
                category="preview_source_missing",
                summary="runtime preview requires a retained result commit",
            )
        port = _free_loopback_port()
        runtime_root = self._runtime.settings.preview_site_root / ".runtime"
        runtime_root.mkdir(parents=True, exist_ok=True)
        home = runtime_root / "home" / task.project_id
        home.mkdir(parents=True, exist_ok=True)
        log_path = runtime_root / f"{task.project_id}.log"
        log = log_path.open("ab", buffering=0)
        environment_variables = {
            "PATH": "/usr/bin:/bin",
            "HOME": str(home),
            "HOST": "127.0.0.1",
            "PORT": str(port),
            "NODE_ENV": "test",
        }
        try:
            process = await asyncio.create_subprocess_exec(
                executable,
                *command[1:],
                cwd=worktree.path,
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
            if process.returncode is None:
                process.terminate()
                await process.wait()
            return StepOutcome(
                kind=OutcomeKind.BLOCKED,
                result={"log_path": str(log_path)},
                category="preview_runtime_unhealthy",
                summary=str(error)[:500],
            )
        target = RuntimeTarget(task.project_id, port, process, worktree.result_commit)
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
                "source_commit": worktree.result_commit,
                "healthcheck": probe,
            }
        )


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
