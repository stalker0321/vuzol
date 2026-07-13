"""Persisted profile snapshots and effective health observations."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.revision import content_revision
from vuzol.providers.domain import EffectiveProfileState, ProviderErrorCategory, QuotaState
from vuzol.providers.errors import ProviderFailure
from vuzol.storage.models import ProfileHealthObservation, ProviderProfile


async def synchronize_profiles(
    session: AsyncSession,
    profiles: tuple[ProviderProfileConfig, ...],
    *,
    configuration_revision: str,
) -> None:
    if not profiles:
        return
    for configured in profiles:
        metadata = configured.model_dump(mode="json", exclude={"credential_reference"})
        metadata["profile_revision"] = content_revision(configured)
        statement = (
            insert(ProviderProfile)
            .values(
                stable_id=configured.id,
                configuration_revision=configuration_revision,
                enabled=configured.enabled,
                metadata_json=metadata,
            )
            .on_conflict_do_update(
                index_elements=[ProviderProfile.stable_id],
                set_={
                    "configuration_revision": configuration_revision,
                    "enabled": configured.enabled,
                    "metadata": metadata,
                    "updated_at": func.now(),
                },
            )
        )
        await session.execute(statement)


async def effective_health(
    session: AsyncSession,
    profile: ProviderProfileConfig,
    *,
    configuration_revision: str,
    now: datetime | None = None,
) -> EffectiveProfileState:
    stored = await session.scalar(
        select(ProviderProfile).where(ProviderProfile.stable_id == profile.id)
    )
    if stored is None:
        return EffectiveProfileState(healthy=False)
    observation = await session.scalar(
        select(ProfileHealthObservation)
        .where(
            ProfileHealthObservation.profile_id == stored.id,
            ProfileHealthObservation.configuration_revision == configuration_revision,
        )
        .order_by(ProfileHealthObservation.observed_at.desc(), ProfileHealthObservation.id.desc())
        .limit(1)
    )
    if observation is None:
        return EffectiveProfileState()
    current = now or datetime.now(UTC)
    unhealthy = not observation.healthy and (
        observation.unhealthy_until is None or observation.unhealthy_until > current
    )
    return EffectiveProfileState(
        healthy=not unhealthy,
        quota_state=QuotaState(observation.quota_state),
        unhealthy_until=observation.unhealthy_until,
        rate_limit_until=observation.rate_limit_until,
    )


async def record_failure_observation(
    session: AsyncSession,
    profile: ProviderProfileConfig,
    *,
    configuration_revision: str,
    failure: ProviderFailure,
) -> None:
    stored = await session.scalar(
        select(ProviderProfile).where(ProviderProfile.stable_id == profile.id)
    )
    if stored is None:
        raise LookupError(f"profile snapshot is missing: {profile.id}")
    now = datetime.now(UTC)
    unhealthy_until = None
    rate_limit_until = None
    quota_state = QuotaState.UNKNOWN
    healthy = True
    if failure.category is ProviderErrorCategory.AUTHENTICATION:
        healthy = False
    elif failure.category is ProviderErrorCategory.QUOTA_EXHAUSTED:
        healthy = False
        quota_state = QuotaState.EXHAUSTED
    elif failure.category is ProviderErrorCategory.RATE_LIMITED:
        delay = failure.retry_after_seconds if failure.retry_after_seconds is not None else 60.0
        rate_limit_until = now + timedelta(seconds=min(delay, 3_600.0))
    elif failure.category in {
        ProviderErrorCategory.TIMEOUT,
        ProviderErrorCategory.PROVIDER_UNAVAILABLE,
        ProviderErrorCategory.UNKNOWN,
    }:
        unhealthy_until = now + timedelta(seconds=30)
        healthy = False
    session.add(
        ProfileHealthObservation(
            profile_id=stored.id,
            configuration_revision=configuration_revision,
            healthy=healthy,
            category=failure.category.value,
            detail={"retryable": failure.retryable},
            unhealthy_until=unhealthy_until,
            rate_limit_until=rate_limit_until,
            quota_state=quota_state.value,
            last_failure_at=func.now(),
        )
    )
    await session.flush()


async def record_success_observation(
    session: AsyncSession,
    profile: ProviderProfileConfig,
    *,
    configuration_revision: str,
) -> None:
    stored = await session.scalar(
        select(ProviderProfile).where(ProviderProfile.stable_id == profile.id)
    )
    if stored is None:
        raise LookupError(f"profile snapshot is missing: {profile.id}")
    session.add(
        ProfileHealthObservation(
            profile_id=stored.id,
            configuration_revision=configuration_revision,
            healthy=True,
            category=None,
            detail={},
            quota_state=QuotaState.UNKNOWN.value,
            last_success_at=func.now(),
        )
    )
    await session.flush()
