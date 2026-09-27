"""Unit tests for the WP10 procedure executor (stages over existing ports)."""

import pytest

from vuzol.projects.procedures import (
    ProcedureRegistry,
    approve_procedure,
    repo_quality_procedure,
)
from vuzol.projects.runner import (
    GateOutcome,
    GateRunner,
    ProcedureRunFailed,
    run_procedure,
)


class _FakeGates(GateRunner):
    def __init__(self, results: dict[str, bool]) -> None:
        self.results = results
        self.calls: list[str] = []

    async def run(self, command_id: str) -> GateOutcome:
        self.calls.append(command_id)
        return GateOutcome(command_id=command_id, passed=self.results[command_id])


def _registry() -> ProcedureRegistry:
    procedure = repo_quality_procedure()
    registry = ProcedureRegistry()
    registry.promote(procedure, approval=approve_procedure(procedure, approver="lead"))
    return registry


def _environment() -> dict[str, tuple[str, str | None]]:
    return {"git": ("installed", "a" * 64), "python-runtime": ("installed", "b" * 64)}


@pytest.mark.anyio
async def test_run_executes_stages_and_builds_receipt() -> None:
    run = await run_procedure(
        _registry(),
        "repo.quality@1",
        gates=("make lint",),
        environment=_environment(),
        gate_runner=_FakeGates({"make lint": True}),
        created_at="2026-09-27T10:00:00Z",
    )
    assert run.procedure_ref == "repo.quality@1"
    assert run.environment == {"git": "installed", "python-runtime": "installed"}
    assert [gate.command_id for gate in run.gates] == ["make lint"]
    assert run.receipt.procedure_ref == "repo.quality@1"
    assert len(run.receipt.receipt_hash) == 64


@pytest.mark.anyio
async def test_unhealthy_environment_stops_before_gates() -> None:
    gates = _FakeGates({"make lint": True})
    with pytest.raises(ProcedureRunFailed) as failure:
        await run_procedure(
            _registry(),
            "repo.quality@1",
            gates=("make lint",),
            environment={"git": ("failed", None), "python-runtime": ("installed", "b" * 64)},
            gate_runner=gates,
            created_at="2026-09-27T10:00:00Z",
        )
    assert failure.value.code == "environment_not_ready"
    assert gates.calls == []
    assert failure.value.receipt is not None


@pytest.mark.anyio
async def test_untrusted_gate_fails_closed() -> None:
    with pytest.raises(ProcedureRunFailed) as failure:
        await run_procedure(
            _registry(),
            "repo.quality@1",
            gates=("rm -rf /",),
            environment=_environment(),
            gate_runner=_FakeGates({}),
            created_at="2026-09-27T10:00:00Z",
        )
    assert failure.value.code == "validation_untrusted_command"


@pytest.mark.anyio
async def test_failed_gate_retains_receipt_evidence() -> None:
    with pytest.raises(ProcedureRunFailed) as failure:
        await run_procedure(
            _registry(),
            "repo.quality@1",
            gates=("make lint",),
            environment=_environment(),
            gate_runner=_FakeGates({"make lint": False}),
            created_at="2026-09-27T10:00:00Z",
        )
    assert failure.value.code == "gate_failed"
    receipt = failure.value.receipt
    assert receipt is not None
    assert receipt.gates[0].gate == "make lint" and receipt.gates[0].passed is False


@pytest.mark.anyio
async def test_unpromoted_procedure_does_not_run() -> None:
    with pytest.raises(ProcedureRunFailed) as failure:
        await run_procedure(
            ProcedureRegistry(),
            "repo.quality@1",
            gates=(),
            environment=_environment(),
            gate_runner=_FakeGates({}),
            created_at="2026-09-27T10:00:00Z",
        )
    assert failure.value.code == "procedure_not_promoted"
