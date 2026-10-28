"""Durable round identity and search indexing are part of the stored contract."""

from vuzol.discussion.domain import DomainError  # noqa: F401
from vuzol.leadership.persistence import LeadershipInference
from vuzol.storage.models import MemoryUnit


def test_round_identity_keeps_a_distinct_receipt_for_each_invocation():
    table = LeadershipInference.__table__
    assert "round" in table.c
    assert not table.c.round.nullable
    assert str(table.c.round.server_default.arg) == "0"
    unique = next(i for i in table.indexes if i.name == "uq_leadership_inference_phase")
    assert [c.name for c in unique.columns] == ["turn_id", "phase", "round"]


def test_decision_search_index_includes_existing_details_without_changing_note_text():
    expression = str(MemoryUnit.__table__.c.text_search.computed.sqltext)
    assert "decision_entry" in expression and "details" in expression
    assert "to_tsvector('simple'" in expression
