"""Revisioned project-name selection before privileged provisioning."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RuntimeConfiguration
from vuzol.interpretation.domain import ProjectNameOption, TaskDraft
from vuzol.storage.models import (
    ProjectNamingRequest,
    ProjectProvisioning,
    Task,
    TelegramIntakeMessage,
    TransactionalOutbox,
)
from vuzol.storage.types import ProjectNamingStatus, ProjectProvisioningStatus, TaskStatus
from vuzol.workflows.transitions import transition_task

if TYPE_CHECKING:
    from vuzol.telegram.domain import ControlUpdate


class ProjectNamingControlError(RuntimeError):
    """A safe user-facing rejection of a stale or unauthorized naming callback."""


class ProjectNamingOutcomeKind(StrEnum):
    SELECTED = "selected"
    REGENERATING = "regenerating"


@dataclass(frozen=True, slots=True)
class ProjectNamingOutcome:
    kind: ProjectNamingOutcomeKind
    request_id: uuid.UUID


class ProjectNamingController:
    def __init__(self, runtime: RuntimeConfiguration) -> None:
        self._runtime = runtime

    async def apply(
        self,
        session: AsyncSession,
        update: ControlUpdate,
    ) -> ProjectNamingOutcome:
        if update.naming_request_id is None or update.naming_revision is None:
            raise ProjectNamingControlError("project naming target is missing")
        naming = await session.get(
            ProjectNamingRequest,
            update.naming_request_id,
            with_for_update=True,
        )
        if naming is None or naming.chat_id != update.chat_id:
            raise ProjectNamingControlError("project naming request not found")
        if naming.requested_by_user_id != update.user_id:
            raise ProjectNamingControlError("only the project author may choose its name")
        if naming.status is not ProjectNamingStatus.PENDING:
            raise ProjectNamingControlError("project naming request is not awaiting a choice")
        if naming.revision != update.naming_revision:
            raise ProjectNamingControlError("project naming options are stale")
        if update.action_kind == "project_name_regenerate":
            return await self._regenerate(session, naming)
        if update.action_kind == "project_name_select":
            return await self._select(session, naming, update)
        raise ProjectNamingControlError("unsupported project naming action")

    async def _regenerate(
        self,
        session: AsyncSession,
        naming: ProjectNamingRequest,
    ) -> ProjectNamingOutcome:
        previous_revision = naming.revision
        naming.revision += 1
        naming.status = ProjectNamingStatus.GENERATING
        naming.last_error_category = None
        session.add(
            TransactionalOutbox(
                destination="telegram",
                operation_type="delete_message",
                linked_entity_type="project_naming",
                linked_entity_id=naming.id,
                idempotency_key=(f"telegram:project-naming:{naming.id}:{previous_revision}:delete"),
                payload={"role": "project_name_options", "revision": previous_revision},
            )
        )
        session.add(
            TransactionalOutbox(
                destination="interpretation",
                operation_type="regenerate_project_names",
                linked_entity_type="project_naming",
                linked_entity_id=naming.id,
                idempotency_key=f"project-naming:generate:{naming.id}:{naming.revision}",
                payload={"revision": naming.revision},
            )
        )
        return ProjectNamingOutcome(ProjectNamingOutcomeKind.REGENERATING, naming.id)

    async def _select(
        self,
        session: AsyncSession,
        naming: ProjectNamingRequest,
        update: ControlUpdate,
    ) -> ProjectNamingOutcome:
        if update.naming_option_index is None:
            raise ProjectNamingControlError("project name option is missing")
        try:
            option = ProjectNameOption.model_validate(naming.options[update.naming_option_index])
        except (IndexError, ValueError) as error:
            raise ProjectNamingControlError("project name option is invalid") from error
        lock_key = int.from_bytes(
            hashlib.sha256(f"project:{option.project_id}".encode()).digest()[:8],
            signed=True,
        )
        await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
        known_ids = {project.id for project in self._runtime.registries.projects.items()}
        conflict = await session.scalar(
            select(ProjectProvisioning.id).where(
                ProjectProvisioning.project_id == option.project_id
            )
        )
        if option.project_id in known_ids or conflict is not None:
            raise ProjectNamingControlError("this project identifier is already in use")
        task = await session.get(Task, naming.task_id, with_for_update=True)
        if task is None or task.status is not TaskStatus.AWAITING_USER:
            raise ProjectNamingControlError("project task is not awaiting a name")
        existing = await session.scalar(
            select(ProjectProvisioning).where(ProjectProvisioning.task_id == task.id)
        )
        if existing is not None:
            raise ProjectNamingControlError("project provisioning already exists")
        provisioning = ProjectProvisioning(
            task_id=task.id,
            requested_by_user_id=naming.requested_by_user_id,
            chat_id=naming.chat_id,
            source_thread_id=naming.source_thread_id,
            project_id=option.project_id,
            display_name=option.display_name,
            description=naming.description,
            repository_path=option.project_id,
            status=ProjectProvisioningStatus.PENDING,
        )
        session.add(provisioning)
        await session.flush()
        naming.status = ProjectNamingStatus.SELECTED
        naming.selected_option_index = update.naming_option_index
        naming.selected_project_id = option.project_id
        naming.selected_display_name = option.display_name
        draft = TaskDraft.model_validate(task.task_draft)
        task.task_draft = draft.model_copy(
            update={
                "new_project_id": option.project_id,
                "new_project_name": option.display_name,
            }
        ).model_dump(mode="json")
        task.project_id = option.project_id
        await transition_task(
            session,
            task,
            TaskStatus.EXECUTING,
            actor_type="project_naming",
            actor_id=str(update.user_id),
            payload={
                "naming_request_id": str(naming.id),
                "project_id": option.project_id,
                "display_name": option.display_name,
            },
        )
        session.add(
            TransactionalOutbox(
                destination="telegram",
                operation_type="delete_message",
                linked_entity_type="project_naming",
                linked_entity_id=naming.id,
                idempotency_key=f"telegram:project-naming:{naming.id}:selected:delete",
                payload={"role": "project_name_options", "revision": naming.revision},
            )
        )
        session.add(
            TransactionalOutbox(
                destination="project_provisioning",
                operation_type="create_project",
                linked_entity_type="project_provisioning",
                linked_entity_id=provisioning.id,
                idempotency_key=f"project:provision:{provisioning.id}",
                payload={"project_id": provisioning.project_id},
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
                    idempotency_key=f"telegram:project:{provisioning.id}:status:{task.version}",
                    payload={"role": "intake_ack", "task_id": str(task.id)},
                )
            )
        return ProjectNamingOutcome(ProjectNamingOutcomeKind.SELECTED, naming.id)
