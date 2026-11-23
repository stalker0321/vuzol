"""Opt-in account substitution without changing a durable model assignment."""

from collections.abc import Callable, Iterable
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig, ProviderRole
from vuzol.config.revision import assignment_revision
from vuzol.providers.catalog import supported_profile
from vuzol.providers.subscription_limits import subscription_account_unusable
from vuzol.storage.models import Step, SubscriptionLimitSnapshotRow
from vuzol.storage.types import StepStatus

# Live 2026-11-23: a dead subscription surfaces as plain "provider_unavailable" calls,
# and only some accounts have a limits collector. Calls on a pool sibling cost nothing
# extra, so a short run of failed calls is enough to move over for a while.
ACCOUNT_FAILURES = frozenset({"authentication", "quota_exhausted", "provider_unavailable"})
FAILURE_RUN = 3
FAILURE_WINDOW = timedelta(minutes=30)


def usable_sibling(
    profile: ProviderProfileConfig,
    profiles: Iterable[ProviderProfileConfig],
    *,
    unusable_accounts: frozenset[str],
    role: ProviderRole,
    extra_check: Callable[[ProviderProfileConfig], bool] | None = None,
) -> ProviderProfileConfig:
    """Return the first compatible usable account, or retain the original profile."""
    if (
        profile.account_pool is None
        or profile.account_id is None
        or profile.account_id not in unusable_accounts
    ):
        return profile
    account_fields = {
        field: getattr(profile, field)
        for field in (
            "id",
            "account_id",
            "account",
            "api_base_url",
            "credential_reference",
            "runtime_identity",
            "state_directory",
        )
    }
    revision = assignment_revision(profile)
    for sibling in sorted(
        profiles, key=lambda candidate: (candidate.routing_priority, candidate.id)
    ):
        if (
            sibling.enabled
            and sibling.account_pool == profile.account_pool
            and sibling.account_id is not None
            and sibling.account_id != profile.account_id
            and sibling.account_id not in unusable_accounts
            and role in sibling.roles
            and supported_profile(sibling)
            and assignment_revision(sibling.model_copy(update=account_fields)) == revision
            and (extra_check is None or extra_check(sibling))
        ):
            return sibling
    return profile


async def unusable_accounts(
    session: AsyncSession,
    profiles: Iterable[ProviderProfileConfig],
    now: datetime,
) -> frozenset[str]:
    """Load fresh definitive account failures, including observations on other models."""
    accounts = {
        profile.id: profile.account_id for profile in profiles if profile.account_id is not None
    }
    if not accounts:
        return frozenset()
    from vuzol.leadership.persistence import LeadershipInference

    since = now - FAILURE_WINDOW
    inferences = await session.execute(
        select(
            LeadershipInference.profile_id,
            LeadershipInference.updated_at,
            LeadershipInference.result["status"].as_string(),
            LeadershipInference.result["failure"]["category"].as_string(),
        ).where(
            LeadershipInference.updated_at >= since,
            LeadershipInference.result.is_not(None),
            LeadershipInference.profile_id.in_(accounts),
        )
    )
    steps = await session.execute(
        select(Step.executor_profile_id, Step.updated_at, Step.status, Step.failure_category).where(
            Step.updated_at >= since,
            Step.status.in_((StepStatus.COMPLETED, StepStatus.FAILED)),
            Step.executor_profile_id.in_(accounts),
        )
    )
    attempts = [
        (accounts[profile_id], at, status == "failed" and category in ACCOUNT_FAILURES)
        for profile_id, at, status, category in inferences
    ] + [
        (
            accounts[profile_id],
            at,
            status is StepStatus.FAILED and category in ACCOUNT_FAILURES,
        )
        for profile_id, at, status, category in steps
    ]
    rows = await session.scalars(
        select(SubscriptionLimitSnapshotRow).where(
            SubscriptionLimitSnapshotRow.profile_id.in_(accounts)
        )
    )
    return frozenset(
        accounts[row.profile_id] for row in rows if subscription_account_unusable(row, now=now)
    ) | failing_accounts(attempts)


def failing_accounts(attempts: Iterable[tuple[str, datetime, bool]]) -> frozenset[str]:
    """Accounts whose latest FAILURE_RUN calls all failed on the account itself."""
    latest: dict[str, list[tuple[datetime, bool]]] = {}
    for account, at, failed in attempts:
        latest.setdefault(account, []).append((at, failed))
    return frozenset(
        account
        for account, calls in latest.items()
        if len(calls) >= FAILURE_RUN
        and all(failed for _, failed in sorted(calls, reverse=True)[:FAILURE_RUN])
    )
