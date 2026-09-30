"""D3 context/budget unit tests (no PostgreSQL)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from vuzol.providers.budgets import purpose_for_step_type


def _item(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "summary": "Step 1",
        "goal": "Goal 1",
        "expected_outcome": "Outcome 1",
        "completion_criteria": ["Check 1"],
        "allowed_scope": "src/**",
        "out_of_scope": [],
        "dependencies": [],
        "trusted_checks": [],
        "suggested_risk": "low",
        "needs_approval": False,
        "estimated_complexity": "small",
        "work_kind": None,
        "capability": None,
        "effect_intent": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_d3_materializer_legacy_null_maps_to_coding() -> None:
    """pp.1: legacy NULL rows keep the previous coding mapping."""

    from vuzol.discussion.sequencer import _task_draft
    from vuzol.interpretation.domain import TaskOperation, TaskType

    draft = _task_draft("vuzol", cast(Any, _item()))
    assert draft.task_type is TaskType.CODING
    assert draft.operation is TaskOperation.MODIFY
    assert {cap.value for cap in draft.required_capabilities} == {
        "repository_read",
        "filesystem_write",
        "code_edit",
    }


def test_d3_materializer_research_read_only() -> None:
    """pp.1/W2: research kind selects the read-only mapping, no write caps."""

    from vuzol.config.models import Capability
    from vuzol.discussion.sequencer import _task_draft
    from vuzol.interpretation.domain import TaskType

    draft = _task_draft(
        "vuzol",
        cast(Any, _item(work_kind="research", capability="web_research")),
    )
    assert draft.task_type is TaskType.RESEARCH
    assert Capability.CODE_EDIT not in draft.required_capabilities
    assert Capability.FILESYSTEM_WRITE not in draft.required_capabilities
    assert Capability.WEB_RESEARCH in draft.required_capabilities


def test_d3_materializer_rejects_unknown_kind_and_write_intent() -> None:
    """pp.1/W2: unsupported kind and read-only/write conflicts fail closed."""

    from vuzol.discussion.domain import DomainError
    from vuzol.discussion.sequencer import _task_draft

    with pytest.raises(DomainError, match="unsupported_work_kind"):
        _task_draft("vuzol", cast(Any, _item(work_kind="deploy")))
    with pytest.raises(DomainError, match="work_kind_effect_mismatch"):
        _task_draft("vuzol", cast(Any, _item(work_kind="scout", effect_intent="modify")))
    with pytest.raises(DomainError, match="unknown_capability"):
        _task_draft("vuzol", cast(Any, _item(work_kind="research", capability="nope")))


def test_d3_purpose_classifies_new_steps_explicitly() -> None:
    """pp.8: new steps never fall silently into default coding."""

    assert purpose_for_step_type("acceptance") == "review"
    assert purpose_for_step_type("scout") == "scout"
    assert purpose_for_step_type("research_execute") == "research"


def test_d3_citation_positions_structural() -> None:
    """W1: citations are offset ranges bound to the raw length."""

    from vuzol.research.source_backed import SourceBackedError, parse_citation_position

    assert parse_citation_position("offset:0-10", raw_length=10) == (0, 10)
    try:
        parse_citation_position("para 3", raw_length=100)
    except SourceBackedError as error:
        assert error.code == "citation_position_not_structural"
    else:
        raise AssertionError("expected refusal")
    try:
        parse_citation_position("offset:90-200", raw_length=100)
    except SourceBackedError as error:
        assert error.code == "citation_range_out_of_bounds"
    else:
        raise AssertionError("expected refusal")
    try:
        parse_citation_position("offset:10-10", raw_length=100)
    except SourceBackedError as error:
        assert error.code == "citation_range_out_of_bounds"
    else:
        raise AssertionError("expected refusal")


def test_d3_assembly_without_retrieval_is_refused() -> None:
    """W1: without retrieval there is no verified research (fail-closed)."""

    from vuzol.research.retrieval import FixtureRetrieval, RetrievedSource
    from vuzol.research.source_backed import SourceBackedError, assemble_source_report

    _fixtures = FixtureRetrieval(fixtures={})
    def fetcher(uri: str, *, now: str) -> RetrievedSource:
        return _fixtures.fetch(uri, now=now)
    try:
        assemble_source_report(
            structured_output={
                "research": {
                    "question": "q?",
                    "sources": [{"uri": "fixture://missing"}],
                    "claims": [
                        {
                            "claim_id": "c1",
                            "statement": "s",
                            "support": "supported",
                            "citations": [["fixture://missing", "offset:0-1"]],
                        }
                    ],
                }
            },
            task_question="q?",
            scope="vuzol",
            created_at="2026-09-30T00:00:00Z",
            fetch=fetcher,
            retriever="local-docs-fixture",
        )
    except SourceBackedError as error:
        assert error.code == "research_retrieval_failed"
    else:
        raise AssertionError("expected refusal without retrieval")


def test_d3_assembly_roundtrip_hash_chain() -> None:
    """W1: sha256(raw) == report hash; D0 consumer accepts the bytes."""

    from vuzol.research.report import validate_source_report_bytes
    from vuzol.research.retrieval import FixtureRetrieval, RetrievedSource
    from vuzol.research.source_backed import assemble_source_report

    raw = b"adapter backed by fixtures"
    _fixtures = FixtureRetrieval(fixtures={"fixture://adapter.md": raw})
    def fetcher(uri: str, *, now: str) -> RetrievedSource:
        return _fixtures.fetch(uri, now=now)
    content, blobs, anchor = assemble_source_report(
        structured_output={
            "research": {
                "question": "Which adapter backs CI?",
                "sources": [{"uri": "fixture://adapter.md"}],
                "claims": [
                    {
                        "claim_id": "c1",
                        "statement": "CI is fixture-based.",
                        "support": "supported",
                        "citations": [["fixture://adapter.md", f"offset:0-{len(raw)}"]],
                    }
                ],
            }
        },
        task_question="Which adapter backs CI?",
        scope="vuzol",
        created_at="2026-09-30T00:00:00Z",
        fetch=fetcher,
        retriever="local-docs-fixture",
    )
    assert blobs[0].content_hash == __import__("hashlib").sha256(raw).hexdigest()
    assert anchor == "2026-09-30T00:00:00Z"
    assert validate_source_report_bytes(content) == ()


def test_d3_scout_request_validation() -> None:
    """W3: scout requests fail closed on unknown kinds and bad bounds."""

    from vuzol.scout import ScoutProbe, ScoutRequest, validate_request

    good = ScoutRequest(
        question="q?",
        scope="vuzol",
        probes=(ScoutProbe(name="p1", kind="fetch", uri="fixture://a"),),
        deadline="2026-10-01T00:00:00Z",
        max_calls=3,
        stop_condition="all_required",
    )
    assert validate_request(good) == ()
    bad_kind = ScoutRequest(
        question="q?",
        scope="vuzol",
        probes=(ScoutProbe(name="p1", kind="repo-exec", uri="x"),),
        deadline="2026-10-01T00:00:00Z",
        max_calls=3,
        stop_condition="all_required",
    )
    assert validate_request(bad_kind) == ("scout_probe_unsupported",)
    assert "scout_max_calls_invalid" in validate_request(
        ScoutRequest(
            question="q?",
            scope="vuzol",
            probes=(ScoutProbe(name="p1", kind="fetch", uri="x"),),
            deadline="2026-10-01T00:00:00Z",
            max_calls=0,
            stop_condition="all_required",
        )
    )


def test_d3_scout_packet_bytes_validate() -> None:
    """W3/Q6: packets are typed immutable bytes with observed revision/time."""

    import json

    from vuzol.scout import validate_scout_packet_bytes

    assert validate_scout_packet_bytes(b"nope") == ("scout_packet_not_json",)
    assert validate_scout_packet_bytes(
        json.dumps({"schema": "scout-packet.v1"}).encode()
    ) == ("scout_packet_status_invalid",)
    full = {
        "schema": "scout-packet.v1",
        "packet_id": "p",
        "request_hash": "r",
        "status": "partial",
        "facts": [{"probe": "p1", "source_hash": "aa" * 32}],
        "observed_revision": "bb" * 32,
        "observed_at": "2026-09-30T00:00:00Z",
        "scope": "vuzol",
    }
    assert validate_scout_packet_bytes(json.dumps(full).encode()) == ()


def test_d3_pair_schema_mismatch_blocks() -> None:
    """W5: a pair slot carrying an undeclared schema fails closed."""

    from types import SimpleNamespace as NS

    from vuzol.context.bindings import validate_resolved_bindings
    from vuzol.context.resolver import BindingError

    resolved = NS(
        bindings=(
            NS(
                slot="scout_packet",
                schema_name="research-result",
                schema_version="research-result.v1",
                content=b"{}",
            ),
        )
    )
    try:
        validate_resolved_bindings(resolved)
    except BindingError as error:
        assert error.category == "pair_schema_mismatch"
    else:
        raise AssertionError("expected pair refusal")
