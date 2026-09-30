"""D5 derived-memory unit tests: identity, status rules, templates, recall."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from vuzol.discussion.domain import DomainError
from vuzol.discussion.memory_units import (
    EXTRACTOR_VERSION,
    MEMORY_DESTINATION,
    RECALLABLE_STATUSES,
    RecallQuery,
    check_status_transition,
    check_unit_type,
    clamp_recall_limit,
    decision_template,
    extraction_identity,
    extraction_scope,
    job_idempotency_key,
    lesson_text,
    observation_text,
    outcome_template,
    should_supersede,
)
from vuzol.interpretation.domain import TaskAction
from vuzol.storage.types import MemoryUnitStatus


def test_job_and_unit_identity_are_stable() -> None:
    trigger = uuid.uuid4()
    session_id = uuid.uuid4()
    scope = extraction_scope(project_id="demo", session_id=session_id)
    assert extraction_scope(project_id=None, session_id=None) == "-:-:"
    first = job_idempotency_key(trigger_event_id=trigger, scope=scope, operation="extract_decision")
    assert (
        job_idempotency_key(trigger_event_id=trigger, scope=scope, operation="extract_decision")
        == first
    )
    assert (
        job_idempotency_key(trigger_event_id=trigger, scope=scope, operation="retract_units")
        != first
    )
    unit = extraction_identity(
        trigger_event_id=trigger,
        scope=scope,
        unit_type="decision_template",
        unit_key="decision:stack",
    )
    assert unit.startswith("memory:")
    assert (
        extraction_identity(
            trigger_event_id=trigger,
            scope=scope,
            unit_type="decision_template",
            unit_key="decision:stack",
        )
        == unit
    )
    with pytest.raises(DomainError, match="unknown unit type"):
        extraction_identity(
            trigger_event_id=trigger, scope=scope, unit_type="embeddings", unit_key="x"
        )
    with pytest.raises(DomainError, match="unit key"):
        extraction_identity(
            trigger_event_id=trigger, scope=scope, unit_type="observation", unit_key="  "
        )
    assert check_unit_type("lesson") == "lesson"


def test_supersession_uses_source_revisions_not_job_order() -> None:
    older = datetime.now(UTC) - timedelta(hours=1)
    newer = datetime.now(UTC)
    assert should_supersede(current_effective_at=older, incoming_effective_at=newer) is True
    assert should_supersede(current_effective_at=newer, incoming_effective_at=older) is False
    # Ties keep the first writer: deterministic under redelivery races.
    assert should_supersede(current_effective_at=newer, incoming_effective_at=newer) is False


def test_hypotheses_never_become_verified() -> None:
    with pytest.raises(DomainError, match="never become verified"):
        check_status_transition(
            source=MemoryUnitStatus.HYPOTHESIS, target=MemoryUnitStatus.VERIFIED
        )
    with pytest.raises(DomainError, match="tombstoned"):
        check_status_transition(
            source=MemoryUnitStatus.TOMBSTONED, target=MemoryUnitStatus.OBSERVATION
        )
    check_status_transition(source=MemoryUnitStatus.HYPOTHESIS, target=MemoryUnitStatus.OBSERVATION)
    check_status_transition(source=MemoryUnitStatus.VERIFIED, target=MemoryUnitStatus.SUPERSEDED)
    check_status_transition(source=MemoryUnitStatus.OBSERVATION, target=MemoryUnitStatus.RETRACTED)
    check_status_transition(source=MemoryUnitStatus.VERIFIED, target=MemoryUnitStatus.TOMBSTONED)
    with pytest.raises(DomainError, match="cannot become"):
        check_status_transition(
            source=MemoryUnitStatus.RETRACTED, target=MemoryUnitStatus.OBSERVATION
        )
    assert (
        frozenset({MemoryUnitStatus.OBSERVATION, MemoryUnitStatus.VERIFIED}) == RECALLABLE_STATUSES
    )


def test_templates_are_deterministic_and_secret_safe() -> None:
    text = decision_template(key="stack", statement="Use Postgres", accepted_by_user_id=7)
    assert "stack" in text and "Use Postgres" in text and "7" in text
    assert decision_template(key="stack", statement="Use Postgres", accepted_by_user_id=7) == text
    package_id = uuid.uuid4()
    outcome = outcome_template(
        package_id=package_id, revision_number=3, accepted_by_user_id=7, evidence_hash="ab" * 32
    )
    assert str(package_id) in outcome and "revision 3" in outcome
    waived = outcome_template(
        package_id=package_id, revision_number=3, accepted_by_user_id=7, evidence_hash=None
    )
    assert "waiver" in waived
    with pytest.raises(DomainError, match="secrets"):
        decision_template(key="stack", statement="api_key = abcdef123456", accepted_by_user_id=7)
    assert observation_text(body="  hello  ") == "hello"
    with pytest.raises(DomainError, match="confirmed evidence"):
        lesson_text(body="toolchain mismatch", evidence_present=False)
    assert "toolchain" in lesson_text(body="toolchain mismatch", evidence_present=True)


def test_recall_query_is_bounded() -> None:
    assert clamp_recall_limit(5) == 5
    assert clamp_recall_limit(10_000) == 50
    with pytest.raises(DomainError, match="positive"):
        clamp_recall_limit(0)
    query = RecallQuery(project_id="demo", unit_types=frozenset({"observation"}), query="postgres")
    assert query.limit == 10
    with pytest.raises(DomainError, match="unknown unit types"):
        RecallQuery(unit_types=frozenset({"embeddings"}))
    with pytest.raises(DomainError, match="blank"):
        RecallQuery(query="   ")
    assert MEMORY_DESTINATION == "memory_extract"
    assert EXTRACTOR_VERSION == "memory-extractor.v1"


def test_completion_paths_do_not_read_memory_units() -> None:
    """Writer delay cannot block completion: completion never queries units."""

    from pathlib import Path

    root = Path(__file__).resolve().parents[3] / "src" / "vuzol"
    for relative in ("workflows/dispatch.py", "discussion/sequencer.py"):
        source = (root / relative).read_text(encoding="utf-8").lower()
        assert "memory_units" not in source
        assert "memory_extract" not in source


def test_constraints_present_without_memory() -> None:
    """Live request constraints apply with no memory rows at all."""

    from vuzol.config import Capability, TopicKind
    from vuzol.interpretation.domain import (
        InterpretationInput,
        SuggestedComplexity,
        TaskDraft,
        TaskOperation,
        TaskType,
    )
    from vuzol.interpretation.policy import enforce_interpretation_policy
    from vuzol.storage.types import RiskLevel

    remote = uuid.uuid4()
    policy = enforce_interpretation_policy(
        InterpretationInput(
            original_input="continue the task",
            topic_kind=TopicKind.PERSONAL,
            capability_vocabulary=frozenset(Capability),
        ),
        TaskDraft(
            action=TaskAction.CONTINUE_TASK,
            task_type=TaskType.CODING,
            operation=TaskOperation.MODIFY,
            goal="Continue the work",
            task_summary="Continue the work",
            suggested_complexity=SuggestedComplexity.SMALL,
            suggested_risk=RiskLevel.LOW,
            needs_clarification=False,
            referenced_task_id=remote,
            normalized_title="Continue the work",
        ),
        known_project_ids=frozenset(),
    )
    assert policy.draft.needs_clarification
    assert "unsupported_task_binding" in policy.reasons
