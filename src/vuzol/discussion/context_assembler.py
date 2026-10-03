"""Repository-backed scoped context assembly (J2).

Reads existing durable owners (work packages / edit sessions / accepted
decisions) and projects them into the pure ``context.assembler`` types. It does
not create a second pending owner: consuming a pending interaction delegates to
the existing :class:`WorkPackageService` single-consume path.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.context.assembler import (
    PendingInteractionProjection,
    PendingInteractionSet,
    PendingOption,
    PendingState,
    ProfileFact,
    ProfileFactSource,
    ProfileSlice,
    build_profile_slice,
)
from vuzol.discussion.domain import PlanDraft
from vuzol.discussion.service import RevisionResult, WorkPackageService
from vuzol.storage.repositories.discussion import DiscussionRepository
from vuzol.storage.repositories.work_packages import WorkPackageRepository
from vuzol.storage.unit_of_work import UnitOfWork


def _options_from_body(body: Mapping[str, object]) -> tuple[PendingOption, ...]:
    raw_items = body.get("items")
    if not isinstance(raw_items, list):
        return ()
    options: list[PendingOption] = []
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            continue
        ordinal = raw.get("ordinal")
        summary = raw.get("summary")
        option_id = raw.get("local_id") or raw.get("item_id")
        if (
            not isinstance(ordinal, int)
            or not isinstance(summary, str)
            or not isinstance(option_id, str)
        ):
            continue
        options.append(PendingOption(option_id=option_id, label=summary, ordinal=ordinal))
    return tuple(sorted(options, key=lambda option: option.ordinal))


async def assemble_pending_interactions(
    session: AsyncSession, *, package_id: uuid.UUID
) -> PendingInteractionSet:
    """Project every open edit session of a package (the delivered options)."""

    repository = WorkPackageRepository(session)
    package = await repository.get_package(package_id)
    projections: list[PendingInteractionProjection] = []
    for edit in await repository.open_edit_sessions(package_id=package_id):
        revision = await repository.get_revision(edit.plan_revision_id)
        projections.append(
            PendingInteractionProjection(
                interaction_id=edit.id,
                kind="edit_session",
                project_id=package.project_id,
                principal_id=edit.opened_by_user_id,
                version=edit.session_generation,
                state=PendingState.OPEN,
                expires_at=edit.expires_at,
                delivered_options=_options_from_body(revision.immutable_body),
            )
        )
    return PendingInteractionSet(interactions=tuple(projections))


def _decision_revision_hash(rows: tuple[tuple[str, str, str], ...]) -> str:
    encoded = "|".join(f"{decision_id}:{key}:{statement}" for decision_id, key, statement in rows)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def assemble_profile_slice(
    session: AsyncSession,
    *,
    session_id: uuid.UUID,
    observed: Mapping[str, str] | None = None,
) -> ProfileSlice:
    """Sourced profile from accepted decisions; observed side kept for conflicts.

    The invalidation token covers every active decision (id, key, statement), so
    changing an accepted constraint changes the hash and invalidates the slice.
    """

    decisions = tuple(
        sorted(
            await DiscussionRepository(session).active_decisions(
                session_id=session_id, newest_limit=50
            ),
            key=lambda decision: (decision.key, str(decision.id)),
        )
    )
    observed = observed or {}
    facts = tuple(
        ProfileFact(
            key=decision.key,
            desired=decision.statement,
            observed=observed.get(decision.key),
            source=ProfileFactSource.ACCEPTED_DECISION,
            source_ref=str(decision.id),
            revision_hash=hashlib.sha256(
                f"{decision.id}:{decision.key}:{decision.statement}".encode()
            ).hexdigest(),
        )
        for decision in decisions
    )
    token = _decision_revision_hash(
        tuple((str(decision.id), decision.key, decision.statement) for decision in decisions)
    )
    return build_profile_slice(facts, accepted_revision_hash=token)


async def consume_pending_edit(
    uow: UnitOfWork,
    *,
    projection: PendingInteractionProjection,
    expected_version: int,
    replacement: PlanDraft,
    user_id: int,
    project_id: str,
    horizon_enabled: bool = False,
) -> RevisionResult:
    """Apply a pending edit through the existing domain service (single consume).

    The projection is checked first (foreign project/principal, stale/expired),
    then ``WorkPackageService.apply_item_edit`` re-checks owner/generation/expiry
    under a row lock — there is no second pending owner.
    """

    projection.applies_to(
        project_id=project_id,
        principal_id=user_id,
        expected_version=expected_version,
    )
    return await WorkPackageService(uow).apply_item_edit(
        edit_session_id=projection.interaction_id,
        expected_session_generation=expected_version,
        replacement=replacement,
        user_id=user_id,
        horizon_enabled=horizon_enabled,
    )
