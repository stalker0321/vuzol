"""Compose the project-lead worker from runtime configuration and one provider adapter."""

from __future__ import annotations

import uuid
from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import ProviderProfileConfig, ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import content_revision
from vuzol.config.settings import Settings
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import current_turn, invocation_current
from vuzol.leadership.model import LeadDecision, LeadModelUnavailable, LeadSnapshot
from vuzol.leadership.persistence import LeadershipTurn
from vuzol.leadership.provider_model import ProviderLeadModel
from vuzol.leadership.verification import GoalVerifier
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.projects.model_matrix import ModelAssignment, assignment_snapshot
from vuzol.providers.catalog import supported_profile
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.registry import AdapterRegistry
from vuzol.storage.models import ProjectExecutorPreference


def build_project_lead_worker(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    profiles: ProfileRegistry,
    owner: str,
    default_branches: Mapping[str, str] | None = None,
    adapter: ProviderAdapter | None = None,
    adapter_registry: AdapterRegistry | None = None,
    reviewer_adapter: ProviderAdapter | None = None,
) -> ProjectLeadWorker:
    """The worker entrypoint. Callers substitute the adapter, not the model."""

    selection = _TurnModels(
        session_factory, settings, profiles, adapter, adapter_registry, reviewer_adapter
    )
    return ProjectLeadWorker(
        session_factory,
        owner=owner,
        model=selection,
        enabled=lambda: settings.project_lead.enabled,
        lease_seconds=settings.project_lead.lease_seconds,
        repeat_limit=settings.project_lead.repeat_limit,
        branch_prefix=settings.project_lead.integration_branch_prefix,
        default_branches=default_branches,
        verifier=selection,
    )


class _TurnModels:
    """Resolve settings/project choices once per durable turn, never once per process."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        profiles: ProfileRegistry,
        adapter: ProviderAdapter | None,
        registry: AdapterRegistry | None,
        reviewer_adapter: ProviderAdapter | None,
    ) -> None:
        self._factory, self._settings, self._profiles = factory, settings, profiles
        self._registry = registry
        self._injected = {"lead": adapter, "goal_reviewer": reviewer_adapter}

    async def _pins(self) -> dict[str, ModelAssignment]:
        context = current_turn.get()
        if context is None:
            raise LeadModelUnavailable("lead_profile_unselected")
        async with self._factory.begin() as session:
            if not await invocation_current(session, context):
                raise LeadModelUnavailable("lead_model_unavailable")
            turn = await session.get(LeadershipTurn, context.turn_id, with_for_update=True)
            assert turn is not None
            if turn.model_assignments is None:
                preference = await session.get(ProjectExecutorPreference, context.project_id)
                choices = assignment_snapshot(
                    {} if preference is None else preference.model_assignments or {}
                )
                pins = {}
                for slot, default in (
                    ("lead", self._settings.project_lead.profile_id),
                    ("goal_reviewer", self._settings.project_lead.reviewer_profile_id),
                ):
                    if slot in choices:
                        pins[slot] = choices[slot]
                    elif default is not None:
                        try:
                            profile = self._profiles.get(default)
                        except ValueError as error:
                            raise LeadModelUnavailable("lead_profile_unselected") from error
                        pins[slot] = ModelAssignment(
                            profile_id=profile.id,
                            model=profile.model,
                            reasoning_effort=profile.model_reasoning_effort,
                            content_revision=content_revision(profile),
                        ).model_dump(mode="json")
                turn.model_assignments = pins
            return {
                slot: ModelAssignment.model_validate(raw)
                for slot, raw in turn.model_assignments.items()
            }

    def _resolve(
        self, pin: ModelAssignment, slot: str
    ) -> tuple[ProviderProfileConfig, ProviderAdapter]:
        try:
            profile = self._profiles.get(pin.profile_id)
            role = ProviderRole.LEAD if slot == "lead" else ProviderRole.REVIEWER
            if (
                not profile.enabled
                or role not in profile.roles
                or not supported_profile(profile)
                or content_revision(profile) != pin.content_revision
                or profile.model != pin.model
                or profile.model_reasoning_effort != pin.reasoning_effort
            ):
                raise ValueError("selected profile is disabled, changed or unsupported")
            adapter = self._injected[slot]
            if adapter is None and self._registry is not None:
                adapter = self._registry.get(profile.id)
            if adapter is None:
                raise ValueError("selected transport is unavailable")
            return profile, adapter
        except (ValueError, LookupError) as error:
            raise LeadModelUnavailable("lead_profile_unselected") from error

    async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
        pins = await self._pins()
        if "lead" not in pins:
            raise LeadModelUnavailable("lead_profile_unselected")
        profile, adapter = self._resolve(pins["lead"], "lead")
        return await ProviderLeadModel(
            self._factory,
            profile,
            adapter,
            limits=self._settings.limits,
            timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
        ).decide(snapshot)

    async def verify(self, goal_id: uuid.UUID, *, claim: TurnClaim) -> None:
        pins = await self._pins()
        if "goal_reviewer" not in pins:
            return
        reviewer = pins["goal_reviewer"]
        lead = pins.get("lead")
        profile, adapter = self._resolve(reviewer, "goal_reviewer")
        if lead is not None and reviewer.profile_id == lead.profile_id:
            raise LeadModelUnavailable("lead_profile_unselected")
        await GoalVerifier(
            self._factory,
            profile,
            adapter,
            limits=self._settings.limits,
            timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
        ).verify(goal_id, claim=claim)
