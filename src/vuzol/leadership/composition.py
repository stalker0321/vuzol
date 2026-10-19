"""Compose the project-lead worker from runtime configuration and one provider adapter."""

from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import Settings
from vuzol.leadership.model import LeadModel, UnselectedLeadProfile
from vuzol.leadership.provider_model import ProviderLeadModel
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.providers.ports import ProviderAdapter
from vuzol.providers.registry import AdapterRegistry


def build_project_lead_worker(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    settings: Settings,
    profiles: ProfileRegistry,
    owner: str,
    default_branches: Mapping[str, str] | None = None,
    adapter: ProviderAdapter | None = None,
    adapter_registry: AdapterRegistry | None = None,
) -> ProjectLeadWorker:
    """The worker entrypoint. Callers substitute the adapter, not the model."""

    return ProjectLeadWorker(
        session_factory,
        owner=owner,
        model=_model(session_factory, settings, profiles, adapter, adapter_registry),
        enabled=lambda: settings.project_lead.enabled,
        lease_seconds=settings.project_lead.lease_seconds,
        repeat_limit=settings.project_lead.repeat_limit,
        branch_prefix=settings.project_lead.integration_branch_prefix,
        default_branches=default_branches,
    )


def _model(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    profiles: ProfileRegistry,
    adapter: ProviderAdapter | None,
    adapter_registry: AdapterRegistry | None,
) -> LeadModel:
    profile_id = settings.project_lead.profile_id
    if profile_id is None:
        return UnselectedLeadProfile()
    try:
        profile = profiles.get(profile_id)
    except ValueError:
        return UnselectedLeadProfile()
    if not profile.enabled or ProviderRole.LEAD not in profile.roles:
        return UnselectedLeadProfile()
    resolved = adapter
    if resolved is None and adapter_registry is not None:
        try:
            resolved = adapter_registry.get(profile.id)
        except ValueError:
            resolved = None
    if resolved is None:
        return UnselectedLeadProfile()
    return ProviderLeadModel(session_factory, profile, resolved)
