"""Server-owned conversation selection. No permission to execute is implied."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.config.models import ProviderProfileConfig, ProviderRole, SandboxNetworkMode
from vuzol.config.revision import content_revision
from vuzol.projects.executor_preference import ExecutorPreferenceError
from vuzol.projects.model_matrix import ModelAssignment, assignment_profile, assignment_snapshot
from vuzol.providers.catalog import conversation_compatible
from vuzol.storage.models import ProjectExecutorPreference

ConversationMode = Literal["think", "lead"]


def parse_conversation_command(text: str | None) -> ConversationMode | None:
    parts = (text or "").strip().split()
    if len(parts) == 1:
        command = parts[0].split("@", 1)[0].lower()
        if command == "/think":
            return "think"
        if command == "/lead":
            return "lead"
    return None


class ConversationRoute(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["discussion-chat-route.v1"] = "discussion-chat-route.v1"
    conversation_mode: ConversationMode
    assignment: ModelAssignment

    @property
    def role(self) -> ProviderRole:
        return ProviderRole.LEAD if self.conversation_mode == "lead" else ProviderRole.EXECUTOR

    @property
    def label(self) -> str:
        name = "Руководитель" if self.conversation_mode == "lead" else "Обсуждение"
        effort = (
            f" · {self.assignment.reasoning_effort}" if self.assignment.reasoning_effort else ""
        )
        return f"{name} · {self.assignment.model}{effort}"

    def selected_route(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "conversation_mode": self.conversation_mode,
            "trusted_profile_id": self.assignment.profile_id,
            "profile_revision": self.assignment.content_revision,
        }

    def validate_profile(
        self, runtime: RuntimeConfiguration, *, project_id: str | None = None
    ) -> ProviderProfileConfig:
        if self.conversation_mode == "lead" and not runtime.settings.project_lead.enabled:
            raise ExecutorPreferenceError("project lead is disabled")
        profile = assignment_profile(self.assignment, runtime.registries, self.conversation_mode)
        if not conversation_compatible(profile, self.role):
            raise ExecutorPreferenceError("selected model has no sandboxed read-only chat adapter")
        if project_id is not None:
            from vuzol.execution.egress import compile_proxy_allowlist

            project = runtime.registries.projects.get(project_id)
            sandbox = runtime.registries.sandboxes.get(project.sandbox_profile)
            if sandbox.network_mode is SandboxNetworkMode.HTTPS_PROXY:
                allowed = {(t.hostname, t.port) for t in compile_proxy_allowlist(project.network)}
                if any(
                    (t.hostname, t.port) not in allowed
                    for t in compile_proxy_allowlist(profile.runtime_network)
                ):
                    raise ExecutorPreferenceError(
                        "selected model needs network access not allowed for this project; "
                        "select a compatible model with /model"
                    )
        return profile


async def select_conversation_route(
    session: AsyncSession,
    runtime: RuntimeConfiguration,
    *,
    project_id: str,
    mode: ConversationMode,
) -> ConversationRoute:
    row = await session.get(ProjectExecutorPreference, project_id)
    assignments = assignment_snapshot(row.model_assignments or {}) if row is not None else {}
    if mode in assignments:
        assignment = ModelAssignment.model_validate(assignments[mode])
    else:
        profile_id = (
            runtime.settings.project_lead.profile_id
            if mode == "lead"
            else runtime.settings.chat.profile_id
        )
        if not profile_id:
            raise ExecutorPreferenceError("conversation model is not configured; send /model")
        try:
            profile = runtime.registries.profiles.get(profile_id)
        except RegistryError as error:
            raise ExecutorPreferenceError(
                "conversation profile is unavailable; send /model"
            ) from error
        assignment = ModelAssignment(
            profile_id=profile.id,
            model=profile.model,
            reasoning_effort=profile.model_reasoning_effort,
            content_revision=content_revision(profile),
        )
    route = ConversationRoute(conversation_mode=mode, assignment=assignment)
    route.validate_profile(runtime, project_id=project_id)
    return route


def conversation_provider_role(selected_route: dict[str, Any]) -> ProviderRole:
    mode = selected_route.get("conversation_mode", "think")
    if mode not in {"think", "lead"}:
        raise ValueError("invalid conversation mode")
    return ProviderRole.LEAD if mode == "lead" else ProviderRole.EXECUTOR
