"""Executable procedure stages over existing handler ports (WP10, BF1).

Stage A selects the environment (healthy installations only, run pins
enforced); stage B runs declared gates through an injected gate-runner port
(allowlist enforced); stage C builds the receipt. Failures retain evidence:
ProcedureRunFailed always carries the partial receipt.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.finalization import TRUSTED_GATE_COMMANDS
from vuzol.projects.installations import CapabilityPinMismatch
from vuzol.projects.procedures import ProcedureRegistry
from vuzol.projects.receipts import GateResult, ProcedureReceipt
from vuzol.storage.models import Artifact


@dataclass(frozen=True, slots=True)
class GateOutcome:
    command_id: str
    passed: bool
    detail: str = ""


class GateRunner(Protocol):
    async def run(self, command_id: str) -> GateOutcome: ...


@dataclass(slots=True)
class LocalGateRunner:
    """Real local gate execution, allowlisted commands only, bounded time."""

    workdir: Path
    timeout_seconds: float = 120.0

    async def run(self, command_id: str) -> GateOutcome:
        argv = TRUSTED_GATE_COMMANDS.get(command_id)
        if argv is None:
            raise ProcedureRunFailed(
                "validation_untrusted_command",
                f"gate command is not trusted: {command_id}",
            )
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=self.workdir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            try:
                output, _ = await asyncio.wait_for(
                    process.communicate(), timeout=self.timeout_seconds
                )
            except TimeoutError as error:
                process.kill()
                raise ProcedureRunFailed("gate_timeout", f"gate timed out: {command_id}") from error
            passed = process.returncode == 0
            return GateOutcome(
                command_id=command_id,
                passed=passed,
                detail=(output or b"").decode("utf-8", errors="replace")[-2000:],
            )
        except ProcedureRunFailed:
            raise
        except Exception as error:
            raise ProcedureRunFailed("gate_execution_failed", str(error)) from error


class ProcedureRunFailed(RuntimeError):
    """A procedure stage failed; the partial receipt is retained as evidence."""

    def __init__(
        self,
        code: str,
        message: str | None = None,
        *,
        receipt: ProcedureReceipt | None = None,
    ) -> None:
        self.code = code
        self.receipt = receipt
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class ProcedureRun:
    procedure_ref: str
    descriptor_hash: str
    environment: dict[str, str]
    gates: tuple[GateOutcome, ...]
    receipt: ProcedureReceipt

    @property
    def receipt_bytes(self) -> bytes:
        return self.receipt.canonical_bytes()


def _environment_hash(environment: Mapping[str, tuple[str, str | None]]) -> str:
    encoded = json.dumps(
        {key: environment[key][1] for key in sorted(environment)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


async def run_procedure(
    registry: ProcedureRegistry,
    ref: str,
    *,
    gates: tuple[str, ...],
    environment: Mapping[str, tuple[str, str | None]],
    gate_runner: GateRunner,
    created_at: str,
    pinned_environment_hash: str | None = None,
) -> ProcedureRun:
    """Execute stages A→B→C. Raises ProcedureRunFailed (with receipt) on failure.

    When the caller passes the pinned environment hash, stage A fails closed
    with CapabilityPinMismatch before any gate runs on drift.
    """

    descriptor = registry.lookup(ref)
    if descriptor is None:
        raise ProcedureRunFailed("procedure_not_promoted", f"procedure is not available: {ref}")
    missing = sorted(
        key
        for key in descriptor.requires
        if environment.get(key, ("missing", None))[0] != "installed"
    )
    if missing:
        raise ProcedureRunFailed(
            "environment_not_ready",
            f"capabilities not healthy: {','.join(missing)}",
            receipt=ProcedureReceipt(
                procedure_ref=descriptor.ref,
                descriptor_hash=descriptor.descriptor_hash,
                environment_hash=_environment_hash(environment),
                gates=(),
                created_at=created_at,
            ),
        )
    current_hash = _environment_hash(environment)
    if pinned_environment_hash is not None and pinned_environment_hash != current_hash:
        raise ProcedureRunFailed(
            "run_pin_mismatch",
            f"environment changed under pinned run {ref}",
            receipt=ProcedureReceipt(
                procedure_ref=descriptor.ref,
                descriptor_hash=descriptor.descriptor_hash,
                environment_hash=current_hash,
                gates=(),
                created_at=created_at,
            ),
        ) from CapabilityPinMismatch(f"pinned {pinned_environment_hash} != current {current_hash}")
    outcomes: list[GateOutcome] = []
    for command_id in gates:
        if command_id not in TRUSTED_GATE_COMMANDS:
            raise ProcedureRunFailed(
                "validation_untrusted_command",
                f"gate command is not trusted: {command_id}",
                receipt=ProcedureReceipt(
                    procedure_ref=descriptor.ref,
                    descriptor_hash=descriptor.descriptor_hash,
                    environment_hash=_environment_hash(environment),
                    gates=tuple(
                        GateResult(gate=o.command_id, passed=o.passed, detail=o.detail)
                        for o in outcomes
                    ),
                    created_at=created_at,
                ),
            )
        outcomes.append(await gate_runner.run(command_id))
    receipt = ProcedureReceipt(
        procedure_ref=descriptor.ref,
        descriptor_hash=descriptor.descriptor_hash,
        environment_hash=_environment_hash(environment),
        gates=tuple(
            GateResult(gate=o.command_id, passed=o.passed, detail=o.detail) for o in outcomes
        ),
        created_at=created_at,
    )
    if any(not outcome.passed for outcome in outcomes):
        raise ProcedureRunFailed("gate_failed", "a declared gate failed", receipt=receipt)
    return ProcedureRun(
        procedure_ref=descriptor.ref,
        descriptor_hash=descriptor.descriptor_hash,
        environment={key: value[0] for key, value in environment.items()},
        gates=tuple(outcomes),
        receipt=receipt,
    )


async def publish_receipt_artifact(
    store: ArtifactStore,
    session: AsyncSession,
    *,
    task_id: uuid.UUID,
    run_id: uuid.UUID,
    step_id: uuid.UUID,
    receipt: ProcedureReceipt,
) -> Artifact:
    """Persist a receipt through the WP02 artifact contract (lead decision 1)."""

    return await store.persist(
        session,
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        artifact_type="procedure_receipt",
        content=receipt.canonical_bytes(),
        media_type="application/json",
    )
