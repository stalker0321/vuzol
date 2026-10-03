"""J2 golden tests for the pure scoped context assembler."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from vuzol.context.assembler import (
    AssemblerError,
    PendingInteractionProjection,
    PendingInteractionSet,
    PendingOption,
    PendingState,
    ProfileFact,
    ProfileFactSource,
    RecentWorkEntry,
    RecentWorkWindow,
    TargetCandidateProjection,
    WorkItemKind,
    WorkOutcome,
    assemble_context,
    build_profile_slice,
    is_explicit_approval,
    option_at,
    project_delivered_options,
    seen_candidate_ids,
)

_HASH = "a" * 64


def _entry(rank: int, *, kind: WorkItemKind = WorkItemKind.TASK) -> RecentWorkEntry:
    return RecentWorkEntry(
        ref=f"task:{uuid.uuid5(uuid.NAMESPACE_URL, str(rank))}",
        kind=kind,
        outcome=WorkOutcome.ACCEPTED,
        content_hash=_HASH,
    )


def _candidate(candidate_id: str, *, rank: int = 0) -> TargetCandidateProjection:
    return TargetCandidateProjection(
        candidate_id=candidate_id,
        statement=f"statement {candidate_id}",
        source_ref=f"task:{uuid.uuid5(uuid.NAMESPACE_URL, candidate_id)}",
        revision_hash=_HASH,
        recency_rank=rank,
    )


def _interaction(
    *,
    project_id: str = "vuzol",
    principal_id: int = 42,
    version: int = 1,
    state: PendingState = PendingState.OPEN,
    expires_in: timedelta | None = timedelta(minutes=5),
) -> PendingInteractionProjection:
    return PendingInteractionProjection(
        interaction_id=uuid.uuid4(),
        kind="edit_session",
        project_id=project_id,
        principal_id=principal_id,
        version=version,
        state=state,
        expires_at=None if expires_in is None else datetime.now(UTC) + expires_in,
        delivered_options=(PendingOption(option_id="one", label="One", ordinal=1),),
    )


def test_exact_ref_resolves_outside_recent_window() -> None:
    entries = tuple(_entry(rank) for rank in range(7))
    window = RecentWorkWindow(entries=entries, limit=5)
    assert len(window.recent) == 5
    outside = entries[6]
    assert window.exact(outside.ref) == outside
    assert window.exact("task:missing") is None


def test_delivered_order_independent_of_candidate_array_order() -> None:
    candidates = (_candidate("alpha"), _candidate("beta"), _candidate("gamma"))
    delivered_order = ("gamma", "alpha")
    forward = project_delivered_options(candidates, delivered_order=delivered_order)
    backward = project_delivered_options(
        tuple(reversed(candidates)), delivered_order=delivered_order
    )

    by_id_forward = {candidate.candidate_id: candidate for candidate in forward}
    by_id_backward = {candidate.candidate_id: candidate for candidate in backward}
    assert by_id_forward["gamma"].delivered_ordinal == 0
    assert by_id_forward["alpha"].delivered_ordinal == 1
    assert by_id_backward["gamma"].delivered_ordinal == 0
    assert by_id_backward["alpha"].delivered_ordinal == 1
    assert option_at(forward, 2).candidate_id == "alpha"
    assert option_at(backward, 2).candidate_id == "alpha"


def test_undelivered_candidates_are_not_seen() -> None:
    candidates = (_candidate("alpha"), _candidate("beta"))
    projected = project_delivered_options(candidates, delivered_order=("alpha",))
    assert seen_candidate_ids(projected) == frozenset({"alpha"})
    beta = next(candidate for candidate in projected if candidate.candidate_id == "beta")
    assert beta.delivered is False and beta.delivered_ordinal is None


def test_unknown_option_position_fails_closed() -> None:
    projected = project_delivered_options((_candidate("alpha"),), delivered_order=("alpha",))
    with pytest.raises(AssemblerError) as error:
        option_at(projected, 2)
    assert error.value.category == "unknown_option"


def test_multiple_open_pending_requires_resolution() -> None:
    one = PendingInteractionSet(interactions=(_interaction(),))
    assert one.requires_resolution is False
    two = PendingInteractionSet(interactions=(_interaction(), _interaction(principal_id=7)))
    assert two.requires_resolution is True
    expired = PendingInteractionSet(
        interactions=(_interaction(), _interaction(expires_in=timedelta(seconds=-1)))
    )
    assert expired.requires_resolution is False


def test_pending_applies_to_rejects_foreign_and_stale() -> None:
    interaction = _interaction(project_id="vuzol", principal_id=42, version=3)
    interaction.applies_to(project_id="vuzol", principal_id=42, expected_version=3)

    with pytest.raises(AssemblerError) as foreign_project:
        interaction.applies_to(project_id="other", principal_id=42, expected_version=3)
    assert foreign_project.value.category == "foreign_project"

    with pytest.raises(AssemblerError) as foreign_principal:
        interaction.applies_to(project_id="vuzol", principal_id=7, expected_version=3)
    assert foreign_principal.value.category == "foreign_principal"

    with pytest.raises(AssemblerError) as stale_version:
        interaction.applies_to(project_id="vuzol", principal_id=42, expected_version=2)
    assert stale_version.value.category == "stale_option"

    expired = _interaction(expires_in=timedelta(seconds=-1))
    with pytest.raises(AssemblerError) as stale_expiry:
        expired.applies_to(project_id="vuzol", principal_id=42, expected_version=1)
    assert stale_expiry.value.category == "stale_option"


def test_profile_slice_keeps_conflict_and_invalidates() -> None:
    facts = (
        ProfileFact(
            key="database",
            desired="postgres",
            observed="sqlite",
            source=ProfileFactSource.ACCEPTED_DECISION,
            source_ref="decision:1",
            revision_hash=_HASH,
        ),
        ProfileFact(
            key="language",
            desired="python",
            observed="python",
            source=ProfileFactSource.CONFIG,
            source_ref="config:1",
            revision_hash=_HASH,
        ),
    )
    slice_value = build_profile_slice(facts, accepted_revision_hash="rev-a")
    assert slice_value.unresolved_gaps == ("database",)
    assert "config:config:1" in slice_value.source_dependencies
    assert slice_value.is_current("rev-a")
    assert not slice_value.is_current("rev-b")


def test_assemble_context_packet_golden() -> None:
    entry = _entry(0)
    candidates = project_delivered_options(
        (_candidate("alpha"), _candidate("beta")), delivered_order=("beta",)
    )
    packet = assemble_context(
        decision_kind="target_resolution",
        recent=RecentWorkWindow(entries=(entry,), limit=5),
        pending=PendingInteractionSet(interactions=(_interaction(),)),
        candidates=candidates,
    )
    assert packet.schema_version == "decision-context.v1"
    assert packet.decision_kind == "target_resolution"
    assert packet.recent.recent == (entry,)
    assert {candidate.candidate_id for candidate in packet.candidates} == {"alpha", "beta"}
    assert packet.pending.open_at()[0].delivered_options[0].option_id == "one"
    assert packet.incomplete is False

    unresolved = assemble_context(
        decision_kind="target_resolution",
        recent=RecentWorkWindow(entries=(entry,), limit=5),
        pending=PendingInteractionSet(interactions=(_interaction(), _interaction(principal_id=7))),
    )
    assert unresolved.incomplete is True


def test_bare_yes_never_consumes_approval() -> None:
    assert is_explicit_approval(None) is False
    for text in ("да", "yes", "ок", "окей", "хорошо"):
        assert is_explicit_approval(text) is False
    assert is_explicit_approval("/approve") is True
    assert is_explicit_approval("подтверждаю") is True
