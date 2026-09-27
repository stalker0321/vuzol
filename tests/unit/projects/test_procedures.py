"""Unit tests for WP10 procedure descriptors, drafts and promotion."""

import pytest

from vuzol.projects.procedures import (
    PROCEDURE_DESCRIPTORS_SCHEMA,
    DraftStore,
    ProcedureRegistry,
    ProcedureStage,
    ProcedureStatus,
    repo_quality_procedure,
)


def test_repo_quality_has_pinned_environment_gates_report_stages() -> None:
    procedure = repo_quality_procedure()
    assert procedure.ref == "repo.quality@1"
    assert [step.stage for step in procedure.stages] == [
        ProcedureStage.ENVIRONMENT,
        ProcedureStage.GATES,
        ProcedureStage.REPORT,
    ]
    assert procedure.canonical()["schema_version"] == PROCEDURE_DESCRIPTORS_SCHEMA
    assert procedure.descriptor_hash == repo_quality_procedure().descriptor_hash


def test_draft_invisible_to_other_tasks_until_promoted() -> None:
    procedure = repo_quality_procedure()
    drafts = DraftStore()
    registry = ProcedureRegistry()
    drafts.save_draft(procedure, author="alice")
    assert drafts.draft_for(procedure.ref, author="alice") == procedure
    assert drafts.draft_for(procedure.ref, author="bob") is None
    assert registry.lookup(procedure.ref) is None
    registry.promote(procedure)
    assert registry.lookup(procedure.ref) == procedure
    assert registry.list_promoted() == (procedure.ref,)


def test_revoked_procedure_no_longer_resolves() -> None:
    procedure = repo_quality_procedure()
    registry = ProcedureRegistry()
    registry.promote(procedure)
    registry.revoke(procedure.ref)
    assert registry.lookup(procedure.ref) is None
    assert registry.list_promoted() == ()
    with pytest.raises(KeyError, match="unknown procedure"):
        registry.revoke("repo.quality@999")


def test_procedure_status_defaults() -> None:
    assert ProcedureStatus.DRAFT.value == "draft"
    assert ProcedureStatus.PROMOTED.value == "promoted"
    assert ProcedureStatus.REVOKED.value == "revoked"
