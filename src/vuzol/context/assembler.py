"""Scoped decision-context assembler (J2).

Projects *existing* durable state into a bounded context packet. This module is
pure: it never reads or writes the database and never grants authority. The
repository-backed assembly lives in ``discussion/context_assembler.py`` and
delegates any consume to the existing domain service (single consume).

Design rules (IMPLEMENTATION_PLAN §J2):

- ``TargetCandidateProjection`` is its own type; the discussion
  ``DecisionCandidate`` is not reused.
- The recency window shows the last N items, but an exact reference is always
  resolvable even when the target sits outside that window.
- Delivered options are projected in the order they were actually shown; an
  undelivered list is never treated as seen.
- Several open pending interactions require explicit resolution.
- A plain affirmation never consumes an approval.
- A profile slice is invalidated when its accepted-decision revision changes.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from vuzol.context.decision_binding import Coverage
from vuzol.context.models import FrozenContextModel

ASSEMBLER_SCHEMA = "decision-context.v1"

_BARE_AFFIRMATIONS = frozenset(
    {"да", "yes", "ок", "окей", "хорошо", "конечно", "ага", "угу", "ok"}  # noqa: RUF001
)
_EXPLICIT_APPROVALS = frozenset({"подтверждаю", "approve", "approved", "подтверждаю."})


class AssemblerError(ValueError):
    """Fail-closed rejection while assembling or applying a projection."""

    def __init__(self, category: str, message: str | None = None) -> None:
        self.category = category
        super().__init__(message or category)


class WorkItemKind(StrEnum):
    TASK = "task"
    PLAN_REVISION = "plan_revision"
    ARTIFACT = "artifact"


class WorkOutcome(StrEnum):
    FAILED = "failed"
    CANCELLED = "cancelled"
    ACCEPTED = "accepted"
    ACTIVE = "active"
    UNKNOWN = "unknown"


class RecentWorkEntry(FrozenContextModel):
    ref: str
    kind: WorkItemKind
    outcome: WorkOutcome
    revision: str | None = None
    content_hash: str


class RecentWorkWindow(FrozenContextModel):
    """All known work ordered by recency; only ``limit`` items are "recent"."""

    entries: tuple[RecentWorkEntry, ...] = ()
    limit: int = 5

    @property
    def recent(self) -> tuple[RecentWorkEntry, ...]:
        return self.entries[: self.limit]

    def exact(self, ref: str) -> RecentWorkEntry | None:
        """Resolve a target by exact ref, even outside the recent window."""

        for entry in self.entries:
            if entry.ref == ref:
                return entry
        return None


class TargetCandidateProjection(FrozenContextModel):
    """A target candidate shown to (or known for) the user. Not DecisionCandidate."""

    candidate_id: str
    statement: str
    source_ref: str
    revision_hash: str
    relevance: int = 0
    recency_rank: int = 0
    delivered: bool = False
    delivered_ordinal: int | None = None

    @property
    def seen(self) -> bool:
        return self.delivered


def project_delivered_options(
    candidates: tuple[TargetCandidateProjection, ...],
    *,
    delivered_order: tuple[str, ...],
) -> tuple[TargetCandidateProjection, ...]:
    """Mark which candidates were delivered and in which order.

    The delivered ordinal comes from ``delivered_order`` (what the user saw),
    never from the candidate array position, so a reversed array cannot change
    "the second option".
    """

    positions = {candidate_id: index for index, candidate_id in enumerate(delivered_order)}
    projected: list[TargetCandidateProjection] = []
    for candidate in candidates:
        ordinal = positions.get(candidate.candidate_id)
        projected.append(
            candidate.model_copy(
                update={
                    "delivered": ordinal is not None,
                    "delivered_ordinal": ordinal,
                }
            )
        )
    return tuple(projected)


def option_at(
    candidates: tuple[TargetCandidateProjection, ...], position: int
) -> TargetCandidateProjection:
    """Return the delivered option at a 1-based position the user referred to."""

    if position < 1:
        raise AssemblerError("invalid_position", "position must be 1-based")
    delivered = sorted(
        (candidate for candidate in candidates if candidate.delivered),
        key=lambda candidate: candidate.delivered_ordinal or 0,
    )
    if position > len(delivered):
        raise AssemblerError("unknown_option", "no delivered option at that position")
    return delivered[position - 1]


def seen_candidate_ids(
    candidates: tuple[TargetCandidateProjection, ...],
) -> frozenset[str]:
    """Only delivered candidates count as seen; undelivered stay unseen."""

    return frozenset(candidate.candidate_id for candidate in candidates if candidate.delivered)


class PendingState(StrEnum):
    OPEN = "open"
    CLOSED = "closed"
    ACCEPTED = "accepted"
    EXPIRED = "expired"


class PendingOption(FrozenContextModel):
    option_id: str
    label: str
    ordinal: int


class PendingInteractionProjection(FrozenContextModel):
    interaction_id: uuid.UUID
    kind: str
    project_id: str
    principal_id: int
    version: int
    state: PendingState
    expires_at: datetime | None = None
    delivered_options: tuple[PendingOption, ...] = ()

    def is_open_at(self, now: datetime | None = None) -> bool:
        if self.state is not PendingState.OPEN:
            return False
        if self.expires_at is None:
            return True
        return self.expires_at > (now or datetime.now(UTC))

    def applies_to(
        self,
        *,
        project_id: str,
        principal_id: int,
        expected_version: int,
        now: datetime | None = None,
    ) -> None:
        """Fail closed for a foreign project/principal or a stale/consumed option."""

        if self.project_id != project_id:
            raise AssemblerError("foreign_project", "interaction belongs to another project")
        if self.principal_id != principal_id:
            raise AssemblerError("foreign_principal", "interaction belongs to another user")
        if not self.is_open_at(now):
            raise AssemblerError("stale_option", "pending interaction is not open")
        if self.version != expected_version:
            raise AssemblerError("stale_option", "pending interaction generation is stale")


class PendingInteractionSet(FrozenContextModel):
    interactions: tuple[PendingInteractionProjection, ...] = ()

    def open_at(self, now: datetime | None = None) -> tuple[PendingInteractionProjection, ...]:
        return tuple(item for item in self.interactions if item.is_open_at(now))

    @property
    def requires_resolution(self) -> bool:
        """More than one open pending interaction needs an explicit choice."""

        return len(self.open_at()) > 1


class ProfileFactSource(StrEnum):
    CONFIG = "config"
    ACCEPTED_DECISION = "accepted_decision"
    DOC = "doc"


class ProfileFact(FrozenContextModel):
    key: str
    desired: str | None = None
    observed: str | None = None
    source: ProfileFactSource
    source_ref: str
    revision_hash: str


class ProfileSlice(FrozenContextModel):
    """Sourced profile facts plus unresolved desired/observed conflicts."""

    facts: tuple[ProfileFact, ...] = ()
    unresolved_gaps: tuple[str, ...] = ()
    source_dependencies: tuple[str, ...] = ()
    accepted_revision_hash: str

    def is_current(self, accepted_revision_hash: str) -> bool:
        return self.accepted_revision_hash == accepted_revision_hash


def build_profile_slice(
    facts: tuple[ProfileFact, ...],
    *,
    accepted_revision_hash: str,
) -> ProfileSlice:
    """Keep both desired and observed sides; surface a gap instead of resolving it."""

    gaps: list[str] = []
    dependencies: list[str] = []
    for fact in facts:
        dependencies.append(f"{fact.source.value}:{fact.source_ref}")
        if fact.desired is not None and fact.observed is not None and fact.desired != fact.observed:
            gaps.append(fact.key)
    return ProfileSlice(
        facts=facts,
        unresolved_gaps=tuple(sorted(set(gaps))),
        source_dependencies=tuple(sorted(set(dependencies))),
        accepted_revision_hash=accepted_revision_hash,
    )


class AssembledContext(FrozenContextModel):
    """Deterministic context packet handed to a semantic producer."""

    schema_version: str = ASSEMBLER_SCHEMA
    decision_kind: str
    recent: RecentWorkWindow
    candidates: tuple[TargetCandidateProjection, ...] = ()
    pending: PendingInteractionSet
    profile: ProfileSlice | None = None
    coverage: Coverage = Coverage.UNKNOWN
    incomplete: bool = False


def assemble_context(
    *,
    decision_kind: str,
    recent: RecentWorkWindow,
    pending: PendingInteractionSet,
    candidates: tuple[TargetCandidateProjection, ...] = (),
    profile: ProfileSlice | None = None,
    coverage: Coverage = Coverage.UNKNOWN,
) -> AssembledContext:
    """Bundle the projections into one packet; several pending stay unresolved."""

    return AssembledContext(
        decision_kind=decision_kind,
        recent=recent,
        candidates=candidates,
        pending=pending,
        profile=profile,
        coverage=coverage,
        incomplete=pending.requires_resolution,
    )


def is_explicit_approval(text: str | None) -> bool:
    """True only for an explicit approval control; a bare "yes" never qualifies.

    This keeps a plain affirmation from consuming an approval through the
    existing ``ApprovalRepository.consume`` path.
    """

    if not text:
        return False
    normalized = text.strip().casefold()
    if normalized in _BARE_AFFIRMATIONS:
        return False
    if normalized.startswith("/approve"):
        return True
    return normalized in _EXPLICIT_APPROVALS
