"""Create a project the owner approved in the assistant topic: the privileged host part.

The interpreter has already created the forum topic. This root unit then creates
the repository and appends the project and its topic to the registry overlay,
copied from a template project. It maps the topic, opens its discussion and
copies the template's model choices. It restarts what caches the registry and
tells the assistant. Every step is safe to repeat after a crash.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tomllib
import uuid
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import (
    ConfigurationBundle,
    ConfigurationLoadError,
    ProjectConfig,
    RegistryDocument,
    RuntimeConfiguration,
    TopicConfig,
    TopicKind,
    build_bundle,
    load_document,
    merge_documents,
)
from vuzol.execution.git import GitError, LocalGit
from vuzol.observability import get_logger
from vuzol.projects.provisioning import _match_repository_root_ownership
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    retry_outbox_item,
)
from vuzol.storage.models import (
    ModelCall,
    ProjectExecutorPreference,
    Step,
    TopicMapping,
    TransactionalOutbox,
)
from vuzol.storage.repositories.discussion import DiscussionRepository
from vuzol.storage.types import StepStatus

PROJECT_SETUP_DESTINATION = "project_setup"
_ATTEMPTS = 3
_LOG = get_logger(__name__)

Runner = Callable[[Sequence[str]], Awaitable[None]]


class SetupRequest(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    entry_id: uuid.UUID
    project_id: str = Field(pattern=r"^[a-z][a-z0-9-]{1,39}$")
    name: str = Field(min_length=1, max_length=60, pattern=r"^[^\n\r\t]+$")
    description: str = Field(min_length=1, max_length=1000)
    chat_id: int
    thread_id: int = Field(ge=1)


def new_project(template: ProjectConfig, request: SetupRequest) -> ProjectConfig:
    return template.model_copy(
        update={
            "id": request.project_id,
            "display_name": request.name,
            "repository_path": Path(request.project_id),
            "default_branch": "main",
            "summary_path": Path("README.md"),
            "static_deployment": None,
            "enabled": True,
        }
    )


def new_topic(request: SetupRequest) -> TopicConfig:
    return TopicConfig(
        chat_id=request.chat_id,
        message_thread_id=request.thread_id,
        kind=TopicKind.PROJECT,
        display_name=request.name,
        project_id=request.project_id,
        accepts_new_tasks=True,
        default_workflow="adaptive_task",
        pinned=False,
    )


def _toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, str):
        # A JSON string with non-ASCII kept is a valid TOML basic string.
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{k} = {_toml_value(v)}" for k, v in value.items()) + " }"
    raise TypeError(f"unsupported registry value: {type(value).__name__}")


def _toml_table(header: str, path: str, data: dict[str, Any], lines: list[str]) -> None:
    lines.append(header)
    nested: list[tuple[str, Any]] = []
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, dict) or (
            isinstance(value, list) and value and all(isinstance(i, dict) for i in value)
        ):
            nested.append((key, value))
        else:
            lines.append(f"{key} = {_toml_value(value)}")
    for key, value in nested:
        lines.append("")
        if isinstance(value, dict):
            _toml_table(f"[{path}.{key}]", f"{path}.{key}", value, lines)
        else:
            for index, item in enumerate(value):
                if index:
                    lines.append("")
                _toml_table(f"[[{path}.{key}]]", f"{path}.{key}", item, lines)


def render_block(project: ProjectConfig, topic: TopicConfig, *, note: str) -> str:
    lines = [f"# {note}"]
    _toml_table("[[projects]]", "projects", project.model_dump(mode="json"), lines)
    lines.append("")
    _toml_table("[[topics]]", "topics", topic.model_dump(mode="json"), lines)
    block = "\n".join(lines) + "\n"
    parsed = RegistryDocument.model_validate(tomllib.loads(block))
    if parsed.projects != (project,) or parsed.topics != (topic,):
        raise ValueError("rendered registry block does not round-trip")
    return block


class ProjectSetupService:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        run: Runner | None = None,
        git: LocalGit | None = None,
        idle_poll_seconds: float = 10.0,
    ) -> None:
        settings = runtime.settings
        if settings.registry_file is None or settings.registry_overlay_file is None:
            raise ValueError("project setup requires the registry and its overlay file")
        self._runtime = runtime
        self._settings = settings
        self._base = settings.registry_file
        self._overlay = settings.registry_overlay_file
        self._factory = factory
        self._owner = owner
        self._run = run or _run_command
        self._git = git or LocalGit()
        self._idle_poll = idle_poll_seconds

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._settings.project_setup.idle_wait_seconds + 900,
                allowed_destinations=frozenset({PROJECT_SETUP_DESTINATION}),
            )
        if token is None:
            return False
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            payload, attempts = dict(item.payload), item.attempt_count
        try:
            request = SetupRequest.model_validate(payload)
            await self._setup(request)
        except (ValidationError, ValueError, LookupError, GitError, OSError) as error:
            reason = type(error).__name__
            _LOG.warning(
                "project setup failed",
                extra={"event": "project_setup.failed", "error": reason},
                exc_info=True,
            )
            async with self._factory.begin() as session:
                if attempts < _ATTEMPTS and not isinstance(error, ValidationError):
                    await retry_outbox_item(
                        session, token, delay_seconds=30 * attempts, error_category=reason
                    )
                else:
                    await _tell_assistant(session, payload, "project_failed", reason=reason)
                    await dead_letter_outbox_item(session, token, error_category=reason)
            return True
        async with self._factory.begin() as session:
            await _tell_assistant(session, payload, "project_ready")
            await complete_outbox_item(session, token)
        return True

    async def _setup(self, request: SetupRequest) -> None:
        bundle = self._bundle()
        known = {project.id: project for project in bundle.projects.items()}
        topics = {
            (topic.chat_id, topic.message_thread_id): topic for topic in bundle.topics.items()
        }
        topic = topics.get((request.chat_id, request.thread_id))
        registered = request.project_id in known
        if registered and (topic is None or topic.project_id != request.project_id):
            raise ValueError("a project with this id already exists")
        if not registered and topic is not None:
            raise ValueError("the topic already belongs to something else")
        template = bundle.projects.get(self._settings.project_setup.template_project_id)
        root = self._settings.repository_root.resolve()
        repository = (root / request.project_id).resolve()
        repository.relative_to(root)
        if not registered:
            if (repository / ".git").is_dir() and not (repository / "README.md").is_file():
                raise ValueError("the repository directory is taken")
            await self._git.initialize_repository(
                repository, readme=f"# {request.name}\n\n{request.description.strip()}\n"
            )
            _match_repository_root_ownership(repository, root)
            self._append(new_project(template, request), new_topic(request))
        async with self._factory.begin() as session:
            await _map_topic(session, request)
            await _copy_preferences(session, template.id, request.project_id)
        await self._restart()

    def _bundle(self) -> ConfigurationBundle:
        return build_bundle(
            merge_documents(load_document(self._base), load_document(self._overlay)),
            self._settings,
            validate_profile_credentials=False,
        )

    def _append(self, project: ProjectConfig, topic: TopicConfig) -> None:
        block = render_block(project, topic, note=f"Created from the assistant: {project.id}")
        current = self._overlay.read_text()
        temporary = self._overlay.with_name(f".{self._overlay.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text(current.rstrip("\n") + "\n\n" + block)
        try:
            stat = self._overlay.stat()
            os.chown(temporary, stat.st_uid, stat.st_gid)
            os.chmod(temporary, stat.st_mode & 0o7777)
            build_bundle(
                merge_documents(load_document(self._base), load_document(temporary)),
                self._settings,
                validate_profile_credentials=False,
            )
            os.replace(temporary, self._overlay)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    async def _restart(self) -> None:
        setup = self._settings.project_setup
        loop = asyncio.get_running_loop()
        deadline = loop.time() + setup.idle_wait_seconds
        for _ in range(int(setup.idle_wait_seconds / self._idle_poll) + 1):
            if loop.time() >= deadline or await self._idle():
                break
            await asyncio.sleep(self._idle_poll)
        if setup.systemd_units:
            await self._run(("systemctl", "restart", *setup.systemd_units))
        if setup.compose_services:
            await self._run((*setup.compose_command, "restart", *setup.compose_services))

    async def _idle(self) -> bool:
        async with self._factory() as session:
            running = await session.scalar(
                select(func.count()).select_from(Step).where(Step.status == StepStatus.RUNNING)
            )
            calls = await session.scalar(
                select(func.count()).select_from(ModelCall).where(ModelCall.state == "leased")
            )
        return not running and not calls


async def _map_topic(session: AsyncSession, request: SetupRequest) -> None:
    await session.execute(
        postgres_insert(TopicMapping)
        .values(
            chat_id=request.chat_id,
            message_thread_id=request.thread_id,
            topic_kind=TopicKind.PROJECT.value,
            project_id=request.project_id,
            accepts_new_tasks=True,
            default_workflow="adaptive_task",
            enabled=True,
        )
        .on_conflict_do_nothing(index_elements=["chat_id", "message_thread_id"])
    )
    discussions = DiscussionRepository(session)
    if (
        await discussions.active_session_id(
            chat_id=request.chat_id, message_thread_id=request.thread_id
        )
        is None
    ):
        await discussions.create_session(
            project_id=request.project_id,
            chat_id=request.chat_id,
            message_thread_id=request.thread_id,
        )


async def _copy_preferences(session: AsyncSession, template_id: str, project_id: str) -> None:
    template = await session.get(ProjectExecutorPreference, template_id)
    if template is None or await session.get(ProjectExecutorPreference, project_id) is not None:
        return
    session.add(
        ProjectExecutorPreference(
            project_id=project_id,
            mode=template.mode,
            worker_key=template.worker_key,
            reasoning_effort=template.reasoning_effort,
            profile_id=template.profile_id,
            model_assignments=dict(template.model_assignments),
        )
    )
    await session.flush()


async def _tell_assistant(
    session: AsyncSession, payload: dict[str, Any], operation: str, **extra: str
) -> None:
    from vuzol.assistant.ingress import schedule

    entry = str(payload.get("entry_id", ""))
    await schedule(
        session,
        operation=operation,
        key=f"assistant:{operation}:{entry}",
        payload={**payload, **extra},
    )


async def _run_command(argv: Sequence[str], *, timeout_seconds: float = 300) -> None:
    """Run one fixed restart command from settings, bounded in time."""
    process = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
    )
    try:
        _stdout, stderr = await asyncio.wait_for(process.communicate(), timeout_seconds)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise OSError(f"{argv[0]} timed out") from None
    if process.returncode != 0:
        raise OSError(f"{argv[0]} failed: {stderr.decode(errors='replace')[:200]}")


async def run_setup_loop(
    service: ProjectSetupService, *, poll_interval_seconds: float, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            worked = await service.process_one()
        except ConfigurationLoadError:
            _LOG.warning("registry unreadable", extra={"event": "project_setup.registry_error"})
            worked = False
        if not worked:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval_seconds)


async def queue_setup(session: AsyncSession, request: SetupRequest) -> None:
    key = f"project-setup:{request.entry_id}"
    if await session.scalar(
        select(TransactionalOutbox.id).where(TransactionalOutbox.idempotency_key == key)
    ):
        return
    session.add(
        TransactionalOutbox(
            destination=PROJECT_SETUP_DESTINATION,
            operation_type="create",
            linked_entity_type="assistant",
            linked_entity_id=request.entry_id,
            idempotency_key=key,
            payload=request.model_dump(mode="json"),
        )
    )
    await session.flush()
