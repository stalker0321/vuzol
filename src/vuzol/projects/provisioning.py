"""Fenced, bounded provisioning of a repository and Telegram project topic."""

import asyncio
import json
import os
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import (
    Capability,
    DeliveryMode,
    RegistryDocument,
    RuntimeConfiguration,
    TopicConfig,
    TopicKind,
    build_bundle,
    load_document,
    merge_documents,
)
from vuzol.execution.git import GitError, LocalGit
from vuzol.storage.leasing import (
    claim_outbox_item,
    complete_outbox_item,
    dead_letter_outbox_item,
    mark_outbox_ambiguous,
    retry_outbox_item,
)
from vuzol.storage.models import (
    ProjectProvisioning,
    Task,
    TelegramIntakeMessage,
    TransactionalOutbox,
)
from vuzol.storage.records import OutboxLeaseToken
from vuzol.storage.types import ProjectProvisioningStatus, TaskStatus
from vuzol.telegram.workspace import TelegramWorkspaceClient, TopicCreationOutcomeUnknown
from vuzol.workflows.transitions import transition_task

PROJECT_PROVISIONING_DESTINATIONS = frozenset({"project_provisioning"})


class ServiceReloader(Protocol):
    async def reload(self) -> None: ...


class FixedSystemdReloader:
    """Restart only the processes that cache project/topic configuration."""

    _units = (
        "vuzol-executor.service",
        "vuzol-telegram.service",
        "vuzol-telegram-delivery.service",
    )

    async def reload(self) -> None:
        process = await asyncio.create_subprocess_exec(
            "systemctl",
            "try-restart",
            *self._units,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise OSError(f"systemd reload failed: {stderr.decode()[:200]}")


class RegistryOverlayWriter:
    def __init__(self, runtime: RuntimeConfiguration, path: Path) -> None:
        self._runtime = runtime
        self._path = path

    def add_project(self, provisioning: ProjectProvisioning) -> str:
        overlay = load_document(self._path) if self._path.exists() else RegistryDocument()
        existing_projects = {project.id for project in overlay.projects}
        existing_topics = {(topic.chat_id, topic.message_thread_id) for topic in overlay.topics}
        assert provisioning.topic_thread_id is not None
        if provisioning.project_id in existing_projects:
            base_path = self._runtime.settings.registry_file
            if base_path is None:
                raise ValueError("static registry file is required for project provisioning")
            return build_bundle(
                merge_documents(load_document(base_path), overlay),
                self._runtime.settings,
                validate_profile_credentials=False,
            ).revision
        topic_key = (provisioning.chat_id, provisioning.topic_thread_id)
        if topic_key in existing_topics:
            raise ValueError("project topic is already assigned in the dynamic registry")
        template = self._runtime.registries.projects.get(self._runtime.settings.project_template_id)
        project = template.model_copy(
            update={
                "id": provisioning.project_id,
                "display_name": provisioning.display_name,
                "repository_path": Path(provisioning.repository_path),
                "summary_path": None,
                "validation_commands": (),
                "allowed_capabilities": frozenset(
                    {
                        Capability.REPOSITORY_READ,
                        Capability.CODE_EDIT,
                        Capability.GIT,
                        Capability.PROJECT_SHELL,
                    }
                ),
                "git_delivery": template.git_delivery.model_copy(
                    update={
                        "allowed_modes": frozenset(
                            {DeliveryMode.RETAIN, DeliveryMode.PATCH, DeliveryMode.APPLY}
                        ),
                        "approval_required": frozenset({DeliveryMode.APPLY}),
                    }
                ),
            }
        )
        topic = TopicConfig(
            chat_id=provisioning.chat_id,
            message_thread_id=provisioning.topic_thread_id,
            kind=TopicKind.PROJECT,
            display_name=provisioning.display_name,
            project_id=provisioning.project_id,
            accepts_new_tasks=True,
            default_workflow="adaptive_task",
        )
        updated = RegistryDocument(
            projects=(*overlay.projects, project),
            profiles=overlay.profiles,
            topics=(*overlay.topics, topic),
            sandboxes=overlay.sandboxes,
        )
        serialized = json.dumps(
            updated.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        persisted = RegistryDocument.model_validate_json(serialized)
        base_path = self._runtime.settings.registry_file
        if base_path is None:
            raise ValueError("static registry file is required for project provisioning")
        prospective = merge_documents(load_document(base_path), persisted)
        bundle = build_bundle(
            prospective,
            self._runtime.settings,
            validate_profile_credentials=False,
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._path.with_suffix(f".{uuid.uuid4().hex}.tmp")
        temporary.write_text(serialized)
        os.replace(temporary, self._path)
        return bundle.revision


class ProjectProvisioningService:
    def __init__(
        self,
        runtime: RuntimeConfiguration,
        session_factory: async_sessionmaker[AsyncSession],
        telegram: TelegramWorkspaceClient,
        *,
        owner: str,
        reloader: ServiceReloader,
        git: LocalGit | None = None,
    ) -> None:
        overlay = runtime.settings.registry_overlay_file
        if overlay is None:
            raise ValueError("registry_overlay_file is required for project provisioning")
        self._runtime = runtime
        self._factory = session_factory
        self._telegram = telegram
        self._owner = owner
        self._reloader = reloader
        self._git = git or LocalGit()
        self._overlay = RegistryOverlayWriter(runtime, overlay)

    async def process_one(self) -> bool:
        async with self._factory.begin() as session:
            token = await claim_outbox_item(
                session,
                owner=self._owner,
                lease_seconds=self._runtime.settings.workflow.lease_seconds,
                allowed_destinations=PROJECT_PROVISIONING_DESTINATIONS,
            )
        if token is None:
            return False
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            if item is None:
                return True
            attempt_count = item.attempt_count
        try:
            await self._provision(token)
        except TopicCreationOutcomeUnknown:
            await self._block_ambiguous(token, "telegram_topic_outcome_unknown")
        except (GitError, OSError, ValueError) as error:
            await self._retry_or_fail(token, attempt_count, type(error).__name__.lower())
        return True

    async def _provision(self, token: OutboxLeaseToken) -> None:
        async with self._factory() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            provisioning = await session.get(ProjectProvisioning, item.linked_entity_id)
            if provisioning is None:
                raise ValueError("project provisioning record is missing")
            provisioning_id = provisioning.id
            project_id = provisioning.project_id
            display_name = provisioning.display_name
            description = provisioning.description
            chat_id = provisioning.chat_id
            topic_thread_id = provisioning.topic_thread_id
        repository = (self._runtime.settings.repository_root / project_id).resolve()
        repository.relative_to(self._runtime.settings.repository_root.resolve())
        readme = f"# {display_name}\n\n{description.strip()}\n"
        await self._git.initialize_repository(repository, readme=readme)
        _match_repository_root_ownership(repository, self._runtime.settings.repository_root)
        async with self._factory.begin() as session:
            row = await session.get(ProjectProvisioning, provisioning_id, with_for_update=True)
            assert row is not None
            if row.status is ProjectProvisioningStatus.PENDING:
                row.status = ProjectProvisioningStatus.REPOSITORY_CREATED
        if topic_thread_id is None:
            async with self._factory.begin() as session:
                row = await session.get(ProjectProvisioning, provisioning_id, with_for_update=True)
                assert row is not None
                if row.status is ProjectProvisioningStatus.TOPIC_CREATING:
                    raise TopicCreationOutcomeUnknown("topic creation cannot be replayed safely")
                row.status = ProjectProvisioningStatus.TOPIC_CREATING
            topic_thread_id = await self._telegram.create_topic(
                chat_id=chat_id,
                name=display_name,
            )
            async with self._factory.begin() as session:
                row = await session.get(ProjectProvisioning, provisioning_id, with_for_update=True)
                assert row is not None
                row.topic_thread_id = topic_thread_id
                row.status = ProjectProvisioningStatus.TOPIC_CREATED
        async with self._factory() as session:
            row = await session.get(ProjectProvisioning, provisioning_id)
            assert row is not None
            revision = self._overlay.add_project(row)
        async with self._factory.begin() as session:
            row = await session.get(ProjectProvisioning, provisioning_id, with_for_update=True)
            assert row is not None
            row.configuration_revision = revision
            row.status = ProjectProvisioningStatus.CONFIGURED
        await self._reloader.reload()
        async with self._factory.begin() as session:
            row = await session.get(ProjectProvisioning, provisioning_id, with_for_update=True)
            assert row is not None and row.topic_thread_id is not None
            task = await session.get(Task, row.task_id, with_for_update=True)
            assert task is not None
            row.status = ProjectProvisioningStatus.COMPLETED
            await transition_task(
                session,
                task,
                TaskStatus.COMPLETED,
                actor_type="project_provisioning",
                payload={"project_id": row.project_id, "topic_thread_id": row.topic_thread_id},
            )
            session.add(
                TransactionalOutbox(
                    destination="telegram",
                    operation_type="send_message",
                    linked_entity_type="project_provisioning",
                    linked_entity_id=row.id,
                    idempotency_key=f"telegram:project:{row.id}:welcome",
                    payload={"role": "project_created"},
                )
            )
            intake = await session.scalar(
                select(TelegramIntakeMessage)
                .where(TelegramIntakeMessage.task_id == task.id)
                .order_by(TelegramIntakeMessage.created_at.desc())
                .limit(1)
            )
            if intake is not None:
                session.add(
                    TransactionalOutbox(
                        destination="telegram",
                        operation_type="send_message",
                        linked_entity_type="telegram_intake",
                        linked_entity_id=intake.id,
                        idempotency_key=(f"telegram:project:{row.id}:status:{task.version}"),
                        payload={"role": "intake_ack", "task_id": str(task.id)},
                    )
                )
            await complete_outbox_item(session, token)

    async def _block_ambiguous(self, token: OutboxLeaseToken, category: str) -> None:
        async with self._factory.begin() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            row = await session.get(
                ProjectProvisioning, item.linked_entity_id, with_for_update=True
            )
            assert row is not None
            row.status = ProjectProvisioningStatus.BLOCKED
            row.last_error_category = category
            task = await session.get(Task, row.task_id, with_for_update=True)
            assert task is not None
            await transition_task(
                session,
                task,
                TaskStatus.BLOCKED,
                actor_type="project_provisioning",
                payload={"reason": category},
            )
            await mark_outbox_ambiguous(session, token)

    async def _retry_or_fail(
        self,
        token: OutboxLeaseToken,
        attempt_count: int,
        category: str,
    ) -> None:
        if attempt_count < 3:
            async with self._factory.begin() as session:
                await retry_outbox_item(
                    session,
                    token,
                    delay_seconds=2**attempt_count,
                    error_category=category,
                )
            return
        async with self._factory.begin() as session:
            item = await session.get(TransactionalOutbox, token.item_id)
            assert item is not None
            row = await session.get(
                ProjectProvisioning, item.linked_entity_id, with_for_update=True
            )
            assert row is not None
            row.status = ProjectProvisioningStatus.FAILED
            row.last_error_category = category
            task = await session.get(Task, row.task_id, with_for_update=True)
            assert task is not None
            await transition_task(
                session,
                task,
                TaskStatus.FAILED,
                actor_type="project_provisioning",
                payload={"reason": category},
            )
            await dead_letter_outbox_item(session, token, error_category=category)


async def run_provisioning_loop(
    service: ProjectProvisioningService,
    *,
    poll_interval_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        processed = await service.process_one()
        if not processed:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval_seconds)


def _match_repository_root_ownership(repository: Path, repository_root: Path) -> None:
    owner = repository_root.stat()
    for root, directories, files in os.walk(repository):
        os.chown(root, owner.st_uid, owner.st_gid, follow_symlinks=False)
        for name in (*directories, *files):
            os.chown(Path(root) / name, owner.st_uid, owner.st_gid, follow_symlinks=False)
