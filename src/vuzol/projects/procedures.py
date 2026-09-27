"""Reusable procedures over the capability registry (WP10).

A procedure is a pinned, versioned composition of existing tools — never a
second installer and never a generic executable format. v1 procedures are
code-defined (like WP03 descriptors) plus fixtures.

Lifecycle (minimal, see docs/PROCEDURE_RUNBOOK.md):
- draft: visible only to its author (never listed to other tasks);
- promote: explicit action gated by an approval under current policy;
- revoke: explicit action; failed probes quarantine the installation
  (excluded from selection).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum

PROCEDURE_DESCRIPTORS_SCHEMA = "procedure-descriptors.v1"


class ProcedureApprovalMismatch(RuntimeError):
    """A promotion approval does not bind the procedure being promoted."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.code = "procedure_approval_mismatch"


class ProcedureStage(StrEnum):
    ENVIRONMENT = "environment"
    GATES = "gates"
    REPORT = "report"


class ProcedureStatus(StrEnum):
    DRAFT = "draft"
    PROMOTED = "promoted"
    REVOKED = "revoked"


@dataclass(frozen=True, slots=True)
class ProcedureApproval:
    """Hash-bound promotion approval (lead decision 2, minimal).

    Issuance stays with the current policy/owner; the registry only verifies
    that the presented envelope binds this exact procedure ref + descriptor
    hash to an approver. No approval object, no promotion.
    """

    procedure_ref: str
    descriptor_hash: str
    approver: str
    envelope_hash: str

    def canonical_envelope(self) -> dict[str, object]:
        return {
            "procedure_ref": self.procedure_ref,
            "descriptor_hash": self.descriptor_hash,
            "approver": self.approver,
        }


def approve_procedure(descriptor: ProcedureDescriptor, *, approver: str) -> ProcedureApproval:
    """Issue a promotion approval (caller acts under current policy)."""

    approval = ProcedureApproval(
        procedure_ref=descriptor.ref,
        descriptor_hash=descriptor.descriptor_hash,
        approver=approver,
        envelope_hash="",
    )
    encoded = json.dumps(approval.canonical_envelope(), sort_keys=True, separators=(",", ":"))
    return ProcedureApproval(
        procedure_ref=approval.procedure_ref,
        descriptor_hash=approval.descriptor_hash,
        approver=approval.approver,
        envelope_hash=hashlib.sha256(encoded.encode()).hexdigest(),
    )


@dataclass(frozen=True, slots=True)
class ProcedureStep:
    stage: ProcedureStage
    action: str
    version: str


@dataclass(frozen=True, slots=True)
class ProcedureDescriptor:
    """Pinned procedure definition: id@version, stages, requirements."""

    procedure_id: str
    version: str
    label: str
    stages: tuple[ProcedureStep, ...]
    requires: tuple[str, ...] = ()
    run_pins: tuple[str, ...] = ()

    @property
    def ref(self) -> str:
        return f"{self.procedure_id}@{self.version}"

    def canonical(self) -> dict[str, object]:
        return {
            "schema_version": PROCEDURE_DESCRIPTORS_SCHEMA,
            "procedure_id": self.procedure_id,
            "version": self.version,
            "label": self.label,
            "stages": [
                {"stage": step.stage.value, "action": step.action, "version": step.version}
                for step in self.stages
            ],
            "requires": list(self.requires),
            "run_pins": list(self.run_pins),
        }

    @property
    def descriptor_hash(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


def repo_quality_procedure() -> ProcedureDescriptor:
    """repo.quality@1 from existing tools: environment, declared gates, report."""

    return ProcedureDescriptor(
        procedure_id="repo.quality",
        version="1",
        label="Repository quality",
        stages=(
            ProcedureStep(
                stage=ProcedureStage.ENVIRONMENT,
                action="installation_states",
                version="installations.v1",
            ),
            ProcedureStep(stage=ProcedureStage.GATES, action="trusted_gates", version="gates.v1"),
            ProcedureStep(
                stage=ProcedureStage.REPORT, action="quality_report", version="report.v1"
            ),
        ),
        requires=("git", "python-runtime"),
        run_pins=("environment_hash",),
    )


@dataclass(slots=True)
class ProcedureRegistry:
    """Promoted procedures only. Drafts live in DraftStore, invisible here."""

    _promoted: dict[str, ProcedureDescriptor] = field(default_factory=dict)
    _status: dict[str, ProcedureStatus] = field(default_factory=dict)

    def promote(self, descriptor: ProcedureDescriptor, *, approval: ProcedureApproval) -> None:
        """Explicit promotion gated by a hash-bound approval (lead decision 2).

        The envelope must bind this exact ref + descriptor hash; issuance
        itself stays under the current policy on the caller's side.
        """

        expected = approve_procedure(descriptor, approver=approval.approver)
        if (
            approval.procedure_ref != descriptor.ref
            or approval.descriptor_hash != descriptor.descriptor_hash
            or approval.envelope_hash != expected.envelope_hash
        ):
            raise ProcedureApprovalMismatch(
                f"approval does not bind {descriptor.ref}@{descriptor.descriptor_hash}"
            )
        self._promoted[descriptor.ref] = descriptor
        self._status[descriptor.ref] = ProcedureStatus.PROMOTED

    def revoke(self, ref: str) -> tuple[str, ...]:
        """Revoke and return the capability keys the caller must quarantine.

        Quarantine itself (failed-probe exclusion) is applied by the caller
        through record_installation; the registry only resolves.
        """

        descriptor = self._promoted.get(ref)
        if descriptor is None or self._status.get(ref) is not ProcedureStatus.PROMOTED:
            raise KeyError(f"unknown procedure {ref}")
        self._status[ref] = ProcedureStatus.REVOKED
        return descriptor.requires

    def lookup(self, ref: str) -> ProcedureDescriptor | None:
        """Resolve a promoted, non-revoked procedure. Drafts never resolve here."""

        if self._status.get(ref) is not ProcedureStatus.PROMOTED:
            return None
        return self._promoted.get(ref)

    def list_promoted(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                ref for ref, status in self._status.items() if status is ProcedureStatus.PROMOTED
            )
        )


@dataclass(slots=True)
class DraftStore:
    """Author-private drafts; invisible to ProcedureRegistry lookups."""

    _drafts: dict[str, ProcedureDescriptor] = field(default_factory=dict)
    _authors: dict[str, str] = field(default_factory=dict)

    def save_draft(self, descriptor: ProcedureDescriptor, *, author: str) -> None:
        self._drafts[descriptor.ref] = descriptor
        self._authors[descriptor.ref] = author

    def draft_for(self, ref: str, *, author: str) -> ProcedureDescriptor | None:
        """A draft is visible only to its author."""

        if self._authors.get(ref) != author:
            return None
        return self._drafts.get(ref)
