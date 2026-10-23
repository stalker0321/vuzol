"""Scoped application operations shared by external assistants and MCP.

Transport authentication supplies the grant; tool arguments never supply identity.
The first slice uses existing project discussion sessions for notifications, not
synthetic Telegram identities. Reads do not create command receipts or wakeups.
"""

import uuid
from dataclasses import asdict
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.limits import WorkLimits
from vuzol.leadership.persistence import GoalDelegation, ManagedGoal
from vuzol.leadership.provenance import RequestOrigin
from vuzol.leadership.service import LeadershipCommandService, goal_card_view
from vuzol.storage.models import ProjectDiscussionSession
from vuzol.storage.types import DiscussionSessionStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.workflows.application import Principal


class ProjectAccess(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    user_id: int = Field(gt=0)
    # Explicit existing project -> discussion mapping, selected by the operator.
    projects: dict[str, uuid.UUID]
    allow_submit: bool = False


class WorkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    project_id: str = Field(min_length=1, max_length=100)
    request_id: uuid.UUID
    request: str = Field(min_length=1, max_length=20_000)
    scope: str = Field(min_length=1, max_length=40_000)
    criteria: list[str] = Field(min_length=1, max_length=30)
    work_limits: WorkLimits = Field(default_factory=WorkLimits)


class ProjectApplication:
    def __init__(
        self, runtime: RuntimeConfiguration, factory: async_sessionmaker[AsyncSession]
    ) -> None:
        self.runtime, self.factory = runtime, factory

    def _project(self, access: ProjectAccess, project_id: str) -> None:
        if project_id not in access.projects:
            raise PermissionError("project_not_available")
        if access.user_id not in self.runtime.settings.allowed_user_ids:
            raise PermissionError("principal_not_available")
        try:
            project = self.runtime.registries.projects.get(project_id)
        except (LookupError, RegistryError):
            raise PermissionError("project_not_available") from None
        if not project.enabled:
            raise PermissionError("project_not_available")

    async def list_projects(self, access: ProjectAccess) -> list[dict[str, str]]:
        projects = []
        for project_id in sorted(access.projects):
            try:
                self._project(access, project_id)
            except PermissionError:
                continue
            project = self.runtime.registries.projects.get(project_id)
            projects.append({"project_id": project_id, "name": project.display_name})
        return projects

    async def inspect(
        self, access: ProjectAccess, project_id: str, goal_id: uuid.UUID | None = None
    ) -> dict[str, Any]:
        self._project(access, project_id)
        async with self.factory() as session:
            query = select(ManagedGoal).where(
                ManagedGoal.project_id == project_id,
                ManagedGoal.owner_user_id == access.user_id,
                ManagedGoal.discussion_session_id == access.projects[project_id],
            )
            if goal_id is not None:
                query = query.where(ManagedGoal.id == goal_id)
            goal = await session.scalar(
                query.order_by(ManagedGoal.created_at.desc(), ManagedGoal.id.desc()).limit(1)
            )
            if goal is None:
                if goal_id is not None:
                    raise PermissionError("goal_not_available")
                return {"project_id": project_id, "goal": None}
            delegation = await session.get(GoalDelegation, goal.delegation_id)
            if delegation is None:
                raise PermissionError("goal_not_available")
            return {
                "project_id": project_id,
                "goal": await goal_card_view(session, goal, delegation),
                "request_origin": goal.constraints.get("request_origin"),
            }

    async def submit(
        self, access: ProjectAccess, request: WorkRequest, *, origin: RequestOrigin | None = None
    ) -> dict[str, Any]:
        self._project(access, request.project_id)
        if not access.allow_submit:
            raise PermissionError("submission_not_allowed")
        if any(not item.strip() or len(item) > 4000 for item in request.criteria):
            raise ValueError("criteria_invalid")
        config = self.runtime.settings.project_lead
        project = self.runtime.registries.projects.get(request.project_id)
        session_id = access.projects[request.project_id]
        service = LeadershipCommandService(
            self.factory,
            enabled=config.enabled,
            branch_prefix=config.integration_branch_prefix,
            repeat_limit=config.repeat_limit,
            default_branches={project.id: project.default_branch},
        )
        async with UnitOfWork(self.factory) as uow:
            assert uow.session is not None
            discussion = await uow.session.get(ProjectDiscussionSession, session_id)
            if (
                discussion is None
                or discussion.project_id != project.id
                or discussion.status is not DiscussionSessionStatus.ACTIVE
                or discussion.chat_id not in self.runtime.settings.allowed_chat_ids
            ):
                raise PermissionError("project_session_not_available")
            result = await service.apply_human(
                Principal(user_id=access.user_id, ingress_source="api"),
                LeadCommand(
                    name="enable_managed_goal",
                    project_id=project.id,
                    idempotency_key=f"assistant:submit:{request.request_id}",
                    payload={
                        "session_id": str(session_id),
                        "integration_branch": (
                            f"{config.integration_branch_prefix}{project.id}/{request.request_id.hex}"
                        ),
                        "request": request.request,
                        "scope": request.scope,
                        "criteria": [
                            {"id": f"outcome-{i}", "description": item}
                            for i, item in enumerate(request.criteria, 1)
                        ],
                        "work_limits": request.work_limits.model_dump(),
                        "request_origin": (
                            origin
                            or RequestOrigin(
                                channel="api",
                                display_name="API",
                                request_id=str(request.request_id),
                            )
                        ).model_dump(mode="json"),
                    },
                ),
                uow=uow,
            )
        # Receipt is committed before the transport can acknowledge submission.
        return asdict(result)
