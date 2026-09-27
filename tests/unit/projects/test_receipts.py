"""Unit tests for WP10 procedure receipts and reuse."""

import hashlib
import json

from vuzol.projects.procedures import (
    ProcedureRegistry,
    approve_procedure,
    repo_quality_procedure,
)
from vuzol.projects.receipts import (
    GateResult,
    ProcedureReceipt,
    ReceiptLinkBroken,
    require_receipt_link,
    verify_receipt_link,
)


def _receipt() -> ProcedureReceipt:
    procedure = repo_quality_procedure()
    return ProcedureReceipt(
        procedure_ref=procedure.ref,
        descriptor_hash=procedure.descriptor_hash,
        environment_hash="e" * 64,
        gates=(GateResult(gate="secret-scan", passed=True),),
        created_at="2026-09-27T10:00:00Z",
    )


def test_receipt_hash_links_artifact_content() -> None:
    receipt = _receipt()
    assert len(receipt.receipt_hash) == 64
    assert verify_receipt_link(receipt, receipt.canonical_bytes())
    require_receipt_link(receipt, receipt.canonical_bytes())


def test_changed_bytes_break_link_fail_closed() -> None:
    receipt = _receipt()
    tampered = json.dumps({"tampered": True}).encode()
    assert not verify_receipt_link(receipt, tampered)
    try:
        require_receipt_link(receipt, tampered)
    except ReceiptLinkBroken as error:
        assert error.code == "receipt_link_broken"
    else:
        raise AssertionError("expected ReceiptLinkBroken")


def test_receipt_canonical_bytes_are_stable() -> None:
    assert _receipt().canonical_bytes() == _receipt().canonical_bytes()
    assert hashlib.sha256(_receipt().canonical_bytes()).hexdigest() == _receipt().receipt_hash


def test_second_lookup_reuses_procedure_without_new_setup() -> None:
    procedure = repo_quality_procedure()
    registry = ProcedureRegistry()
    registry.promote(procedure, approval=approve_procedure(procedure, approver="lead"))
    first = registry.lookup(procedure.ref)
    second = registry.lookup(procedure.ref)
    assert first is not None and second is not None
    assert first.descriptor_hash == second.descriptor_hash == procedure.descriptor_hash
