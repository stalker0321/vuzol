"""Compose the project-lead worker from runtime configuration and one provider adapter."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import Capability, LaunchMode, ProviderProfileConfig
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import assignment_matches, assignment_revision
from vuzol.config.settings import ResearchModelChoice, Settings
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import current_turn, invocation_current
from vuzol.leadership.job_runner import ResearchJobRunner
from vuzol.leadership.model import LeadDecision, LeadModelUnavailable, LeadSnapshot
from vuzol.leadership.persistence import LeadershipTurn, LeadJob
from vuzol.leadership.provider_model import ProviderLeadModel
from vuzol.leadership.retrospective import GoalRetrospectiveConsumer
from vuzol.leadership.retrospective_model import RetrospectiveResponse, generate_retrospective
from vuzol.leadership.verification import GoalVerifier
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.projects.executor_preference import (
    PAYLOAD_EFFORT_OVERRIDE,
    PAYLOAD_MODEL_OVERRIDE,
    apply_profile_overrides,
)
from vuzol.projects.model_matrix import SLOT_ROLES, ModelAssignment, assignment_snapshot
from vuzol.providers.catalog import supported_profile
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.registry import AdapterRegistry
from vuzol.storage.models import ProjectExecutorPreference

# Account or transport failures, not a bad plan: another planner may succeed.
_FALLBACK_PLANNER_REASONS = frozenset(
    {
        "planner_quota_exhausted",
        "planner_provider_unavailable",
        "planner_rate_limited",
        "planner_timeout",
        "planner_authentication",
    }
)


def build_project_lead_worker(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    profiles: ProfileRegistry,
    owner: str,
    default_branches: Mapping[str, str] | None = None,
    repository_paths: Mapping[str, Path] | None = None,
    adapter: ProviderAdapter | None = None,
    adapter_registry: AdapterRegistry | None = None,
    reviewer_adapter: ProviderAdapter | None = None,
    planner_adapter: ProviderAdapter | None = None,
) -> ProjectLeadWorker:
    """The worker entrypoint. Callers substitute the adapter, not the model."""

    selection = _TurnModels(
        session_factory,
        settings,
        profiles,
        adapter,
        adapter_registry,
        reviewer_adapter,
        planner_adapter,
        repository_paths,
    )
    return ProjectLeadWorker(
        session_factory,
        owner=owner,
        model=selection,
        enabled=lambda: settings.project_lead.enabled,
        lease_seconds=settings.project_lead.lease_seconds,
        model_retry_delays_seconds=settings.project_lead.model_retry_delays_seconds,
        repeat_limit=settings.project_lead.repeat_limit,
        branch_prefix=settings.project_lead.integration_branch_prefix,
        default_goal_max_cost=settings.project_lead.default_goal_max_cost,
        default_branches=default_branches,
        repository_paths=repository_paths,
        verifier=selection,
        job_runner=ResearchJobRunner(
            session_factory,
            owner=f"{owner}:jobs",
            model=selection,
            enabled=lambda: settings.project_lead.enabled,
            concurrency=settings.project_lead.job_concurrency,
            lease_seconds=settings.project_lead.lease_seconds,
        ),
    )


def build_goal_retrospective_worker(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    profiles: ProfileRegistry,
    owner: str,
    adapter: ProviderAdapter | None = None,
    adapter_registry: AdapterRegistry | None = None,
) -> GoalRetrospectiveConsumer:
    return GoalRetrospectiveConsumer(
        session_factory,
        owner=owner,
        model=_TurnModels(
            session_factory, settings, profiles, adapter, adapter_registry, None, None, None
        ),
        enabled=lambda: settings.project_lead.enabled,
        timeout_seconds=settings.project_lead.inference_timeout_seconds,
        # A model outage lasts minutes to hours (2026-10-27: ~40 minutes); back off for
        # about two hours in total before giving the retrospective up.
        max_attempts=8,
        retry_min_seconds=60,
        retry_max_seconds=3600,
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
        planner_adapter: ProviderAdapter | None,
        repository_paths: Mapping[str, Path] | None,
    ) -> None:
        self._factory, self._settings, self._profiles = factory, settings, profiles
        self._registry = registry
        self._injected = {
            "lead": adapter,
            "goal_reviewer": reviewer_adapter,
            "planner_strong": planner_adapter,
        }
        self._repositories = dict(repository_paths or {})

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
                    ("planner_strong", None),
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
                            content_revision=assignment_revision(profile),
                        ).model_dump(mode="json")
                turn.model_assignments = pins
            return {
                slot: ModelAssignment.model_validate(raw)
                for slot, raw in turn.model_assignments.items()
            }

    async def research(self, job: LeadJob) -> tuple[str, dict[str, Any]]:
        from vuzol.leadership.research import generate_research

        if self._settings.project_lead.research_models:
            # Validate the whole chain before sending, including currently unused fallbacks.
            choices = [
                self._resolve_research(choice)
                for choice in self._settings.project_lead.research_models
            ]
            failures = []
            context = current_turn.get()
            assert context is not None
            for index, (profile, adapter) in enumerate(choices):
                try:
                    report, usage = await generate_research(
                        self._factory,
                        job,
                        profile,
                        adapter,
                        repository=self._repositories.get(context.project_id),
                        limits=self._settings.limits,
                        timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
                        choice_index=index,
                    )
                except LeadModelUnavailable as error:
                    failures.append(
                        {"profile_id": profile.id, "model": profile.model, "reason": str(error)}
                    )
                    if not error.definitive or index == len(choices) - 1:
                        raise LeadModelUnavailable(
                            str(error),
                            retryable=error.retryable,
                            usage={"failed_attempts": failures},
                        ) from error
                else:
                    return report, {
                        **usage,
                        "profile_id": profile.id,
                        "model": profile.model,
                        "reasoning_effort": profile.model_reasoning_effort,
                        "failed_attempts": failures,
                    }
            raise AssertionError("nonempty research chain exhausted without an outcome")
        pins = await self._pins()
        if "planner_strong" not in pins:
            raise LeadModelUnavailable("planner_profile_unselected")
        profile, adapter = self._resolve(pins["planner_strong"], "planner_strong")
        context = current_turn.get()
        assert context is not None
        return await generate_research(
            self._factory,
            job,
            profile,
            adapter,
            repository=self._repositories.get(context.project_id),
            limits=self._settings.limits,
            timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
        )

    def _resolve_research(
        self, choice: ResearchModelChoice
    ) -> tuple[ProviderProfileConfig, ProviderAdapter]:
        try:
            profile = self._profiles.get(choice.profile_id)
            if (
                not profile.enabled
                or Capability.REPOSITORY_READ not in profile.capabilities
                or not supported_profile(profile)
                or (profile.launch_mode is LaunchMode.CLI and not profile.sandbox_required)
            ):
                raise ValueError("research profile must support sandboxed repository reads")
            overrides: dict[str, object] = {}
            if choice.model is not None:
                overrides[PAYLOAD_MODEL_OVERRIDE] = choice.model
            if choice.reasoning_effort is not None:
                overrides[PAYLOAD_EFFORT_OVERRIDE] = choice.reasoning_effort
            profile = apply_profile_overrides(profile, overrides)
            adapter = self._injected["planner_strong"]
            if adapter is None and self._registry is not None:
                adapter = self._registry.get(profile.id)
            if adapter is None:
                raise ValueError("research transport is unavailable")
            return profile, adapter
        except (ValueError, LookupError) as error:
            raise LeadModelUnavailable("research_model_unavailable") from error

    async def retrospect(
        self, project_id: str, context: dict[str, Any], *, invocation_id: uuid.UUID
    ) -> RetrospectiveResponse:
        pins = await self._pins()
        if "lead" not in pins:
            raise LeadModelUnavailable("lead_profile_unselected")
        profile, adapter = self._resolve(pins["lead"], "lead")
        return await generate_retrospective(
            self._factory,
            profile,
            adapter,
            context,
            invocation_id=invocation_id,
            limits=self._settings.limits,
            timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
        )

    def _fallback_planner(self, profile_id: str) -> tuple[ProviderProfileConfig, ProviderAdapter]:
        try:
            profile = self._profiles.get(profile_id)
            if (
                not profile.enabled
                or SLOT_ROLES["planner_strong"] not in profile.roles
                or not supported_profile(profile)
            ):
                raise ValueError("fallback planner is disabled or unsupported")
            adapter = self._injected["planner_strong"]
            if adapter is None and self._registry is not None:
                adapter = self._registry.get(profile.id)
            if adapter is None:
                raise ValueError("fallback planner transport is unavailable")
            return profile, adapter
        except (ValueError, LookupError) as error:
            raise LeadModelUnavailable("planner_fallback_unavailable") from error

    def _resolve(
        self, pin: ModelAssignment, slot: str
    ) -> tuple[ProviderProfileConfig, ProviderAdapter]:
        try:
            profile = self._profiles.get(pin.profile_id)
            role = SLOT_ROLES[slot]
            if (
                not profile.enabled
                or role not in profile.roles
                or not supported_profile(profile)
                or not assignment_matches(profile, pin.content_revision)
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
        if (snapshot.result_context.get("event") or {}).get("wakeup_kind") == "plan_requested":
            from vuzol.leadership.planning import generate_plan, outcome_decision

            try:
                pins = await self._pins()
                if "planner_strong" not in pins:
                    raise LeadModelUnavailable("planner_profile_unselected")
                profile, adapter = self._resolve(pins["planner_strong"], "planner_strong")
            except LeadModelUnavailable:
                return outcome_decision(snapshot, {"reason": "planner_profile_unselected"})
            decision = await generate_plan(
                self._factory,
                snapshot,
                profile,
                adapter,
                repository=self._repositories.get(snapshot.project_id),
                limits=self._settings.limits,
                timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
            )
            fallbacks = self._settings.project_lead.planner_fallback_profile_ids
            for index, fallback_id in enumerate(fallbacks, start=1):
                reason = (decision.planning_outcome or {}).get("reason")
                if reason not in _FALLBACK_PLANNER_REASONS:
                    break
                try:
                    fallback = self._fallback_planner(fallback_id)
                except LeadModelUnavailable:
                    continue
                decision = await generate_plan(
                    self._factory,
                    snapshot,
                    *fallback,
                    repository=self._repositories.get(snapshot.project_id),
                    limits=self._settings.limits,
                    timeout_seconds=self._settings.project_lead.inference_timeout_seconds,
                    fallback_round=index,
                )
            return decision
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
