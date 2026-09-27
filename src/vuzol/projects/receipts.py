"""Procedure receipts published as Artifacts (WP10, lead decision 1).

A receipt is canonical JSON; its sha256 (receipt_hash) is persisted as an
Artifact whose content hash is the same digest by construction. The link
receipt_hash ↔ artifact content hash is explicit and re-verifiable on read:
hash mismatch fails closed. This module is pure; persistence goes through
ArtifactStore.persist (WP02 contract) at the call site.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GateResult:
    gate: str
    passed: bool
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ProcedureReceipt:
    procedure_ref: str
    descriptor_hash: str
    environment_hash: str
    gates: tuple[GateResult, ...]
    created_at: str

    def canonical(self) -> dict[str, object]:
        return {
            "schema": "procedure-receipt.v1",
            "procedure_ref": self.procedure_ref,
            "descriptor_hash": self.descriptor_hash,
            "environment_hash": self.environment_hash,
            "gates": [
                {"gate": gate.gate, "passed": gate.passed, "detail": gate.detail}
                for gate in self.gates
            ],
            "created_at": self.created_at,
        }

    def canonical_bytes(self) -> bytes:
        return json.dumps(self.canonical(), sort_keys=True, separators=(",", ":")).encode()

    @property
    def receipt_hash(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def verify_receipt_link(receipt: ProcedureReceipt, artifact_content: bytes) -> bool:
    """Re-verify the explicit receipt_hash ↔ artifact content hash link."""

    return hashlib.sha256(artifact_content).hexdigest() == receipt.receipt_hash


class ReceiptLinkBroken(RuntimeError):
    """Persisted bytes no longer match the receipt they claim."""

    def __init__(self, code: str = "receipt_link_broken") -> None:
        self.code = code
        super().__init__(code)


def require_receipt_link(receipt: ProcedureReceipt, artifact_content: bytes) -> None:
    if not verify_receipt_link(receipt, artifact_content):
        raise ReceiptLinkBroken()
