import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vuzol.execution.proxy_service import ProxyRecoveryManifest
from vuzol.execution.reconciliation import (
    DurableLeaseState,
    ReconciliationClassification,
    ReconciliationDecision,
    ReconciliationReport,
    classify_recovery_manifest,
)
from vuzol.storage.types import RunStatus, StepStatus

NOW = datetime(2026, 7, 13, tzinfo=UTC)


def _manifest(generation: int = 2) -> ProxyRecoveryManifest:
    return ProxyRecoveryManifest(
        directory=Path("/safe/runtime/identity"),
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        step_id=uuid.uuid4(),
        lease_generation=generation,
        policy_hash="a" * 64,
    )


def _state(
    *,
    generation: int = 2,
    run_status: RunStatus = RunStatus.RUNNING,
    step_status: StepStatus = StepStatus.RUNNING,
    owner: str | None = "executor-a",
    expires_at: datetime | None = NOW + timedelta(minutes=1),
    heartbeat_at: datetime | None = NOW,
    identity_consistent: bool = True,
) -> DurableLeaseState:
    return DurableLeaseState(
        identity_consistent=identity_consistent,
        run_status=run_status,
        step_status=step_status,
        current_generation=generation,
        lease_owner=owner,
        lease_expires_at=expires_at,
        heartbeat_at=heartbeat_at,
    )


@pytest.mark.parametrize("step_status", [StepStatus.LEASED, StepStatus.RUNNING])
def test_current_unexpired_active_lease_is_preserved(step_status: StepStatus) -> None:
    result = classify_recovery_manifest(_manifest(), _state(step_status=step_status), now=NOW)

    assert result is ReconciliationClassification.PRESERVED_ACTIVE_LEASE


@pytest.mark.parametrize(
    ("run_status", "step_status"),
    [
        (RunStatus.COMPLETED, StepStatus.COMPLETED),
        (RunStatus.FAILED, StepStatus.RUNNING),
        (RunStatus.RUNNING, StepStatus.CANCELLED),
    ],
)
def test_terminal_leftover_is_removable(run_status: RunStatus, step_status: StepStatus) -> None:
    result = classify_recovery_manifest(
        _manifest(),
        _state(run_status=run_status, step_status=step_status),
        now=NOW,
    )

    assert result is ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER


def test_expired_current_lease_is_removable() -> None:
    result = classify_recovery_manifest(
        _manifest(), _state(expires_at=NOW - timedelta(seconds=1)), now=NOW
    )

    assert result is ReconciliationClassification.REMOVED_EXPIRED_LEASE


def test_old_generation_is_removable() -> None:
    result = classify_recovery_manifest(_manifest(generation=1), _state(generation=2), now=NOW)

    assert result is ReconciliationClassification.REMOVED_OLD_GENERATION


@pytest.mark.parametrize(
    "state",
    [
        None,
        _state(identity_consistent=False),
        _state(generation=1),
        _state(owner=None),
        _state(expires_at=None),
        _state(heartbeat_at=None),
        _state(step_status=StepStatus.QUEUED),
    ],
)
def test_ambiguous_or_newer_state_fails_closed(state: DurableLeaseState | None) -> None:
    result = classify_recovery_manifest(_manifest(generation=2), state, now=NOW)

    assert result is ReconciliationClassification.PRESERVED_AMBIGUOUS


def test_report_counts_only_successful_removals() -> None:
    report = ReconciliationReport(
        lock_acquired=True,
        decisions=(
            ReconciliationDecision(
                step_id="active",
                lease_generation=1,
                classification=ReconciliationClassification.PRESERVED_ACTIVE_LEASE,
            ),
            ReconciliationDecision(
                step_id="stale",
                lease_generation=1,
                classification=ReconciliationClassification.REMOVED_EXPIRED_LEASE,
            ),
            ReconciliationDecision(
                step_id="failed",
                lease_generation=1,
                classification=ReconciliationClassification.CLEANUP_FAILED,
            ),
        ),
    )

    assert report.removed_count == 1
