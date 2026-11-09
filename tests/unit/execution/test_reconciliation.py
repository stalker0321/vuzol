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


def _reconciler(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.execution.reconciliation import ProxyStartupReconciler

    session = AsyncMock()
    session.add = MagicMock()
    session.begin = MagicMock()
    session.begin.return_value.__aenter__ = AsyncMock(return_value=session)
    session.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    manager = MagicMock()
    for name in (
        "step_network_leases",
        "cleanup_networks",
        "validate_recovery_resources",
        "cleanup_recovery_manifest",
    ):
        setattr(manager, name, AsyncMock())
    manager.step_network_leases.return_value = ()
    reconciler = ProxyStartupReconciler(factory, manager, owner="executor", lock_timeout_seconds=0)
    monkeypatch.setattr(
        reconciler,
        "_read_state_for_update",
        AsyncMock(return_value=_state(step_status=StepStatus.COMPLETED)),
    )
    return reconciler, manager, session


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error,classification",
    [
        ("foreign network", ReconciliationClassification.PRESERVED_FOREIGN),
        ("unreadable manifest", ReconciliationClassification.PRESERVED_AMBIGUOUS),
    ],
)
async def test_reconciliation_preserves_resources_without_exact_ownership(
    monkeypatch, error, classification
):
    from vuzol.execution.proxy_service import ProxyServiceError

    reconciler, manager, session = _reconciler(monkeypatch)
    manager.validate_recovery_resources.side_effect = ProxyServiceError(error)
    decision = await reconciler._reconcile_one(session, _manifest())
    assert decision.classification is classification
    manager.cleanup_recovery_manifest.assert_not_awaited()
    event = session.add.call_args.args[0]
    assert event.payload["classification"] == classification.value
    assert event.actor_id == "executor"


@pytest.mark.anyio
async def test_failed_cleanup_records_failure_without_marking_process_reaped(monkeypatch):
    from vuzol.execution.proxy_networks import ProxyNetworkError

    reconciler, manager, session = _reconciler(monkeypatch)
    manager.cleanup_recovery_manifest.side_effect = ProxyNetworkError("network busy")
    decision = await reconciler._reconcile_one(session, _manifest())
    assert decision.classification is ReconciliationClassification.CLEANUP_FAILED
    session.scalars.assert_not_awaited()
    assert session.add.call_args.args[0].payload["classification"] == "CLEANUP_FAILED"


@pytest.mark.anyio
@pytest.mark.parametrize("source_error", [False, True])
async def test_network_sweep_preserves_unreadable_lease_inventory(monkeypatch, source_error):
    from vuzol.execution.proxy_networks import ProxyNetworkError
    from vuzol.execution.proxy_service import ProxyServiceError

    reconciler, manager, session = _reconciler(monkeypatch)
    manager.step_network_leases.side_effect = (
        ProxyNetworkError("unavailable") if source_error else ProxyServiceError("unavailable")
    )
    assert await reconciler._sweep_unmanifested_networks(set()) == []
    manager.cleanup_networks.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case", ["young", "malformed_date", "manifested", "active", "cleanup_error", "orphan"]
)
async def test_network_sweep_requires_age_and_revocation_evidence(monkeypatch, case):
    from types import SimpleNamespace

    from vuzol.execution.proxy_networks import ProxyNetworkError

    reconciler, manager, session = _reconciler(monkeypatch)
    manifest = _manifest()
    lease = SimpleNamespace(
        task_id=manifest.task_id,
        run_id=manifest.run_id,
        step_id=manifest.step_id,
        lease_generation=manifest.lease_generation,
    )
    created = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    if case == "young":
        created = datetime.now(UTC).isoformat()
    elif case == "malformed_date":
        created = "not a timestamp"
    manager.step_network_leases.return_value = [(lease, created)]
    if case == "active":
        reconciler._read_state_for_update.return_value = _state(
            expires_at=datetime.now(UTC) + timedelta(minutes=1)
        )
    elif case == "orphan":
        reconciler._read_state_for_update.return_value = None
    if case == "cleanup_error":
        manager.cleanup_networks.side_effect = ProxyNetworkError("busy")
    decisions = await reconciler._sweep_unmanifested_networks(
        {manifest.identity} if case == "manifested" else set()
    )
    if case == "orphan":
        assert len(decisions) == 1
        assert decisions[0].classification is ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER
        manager.cleanup_networks.assert_awaited_once_with(lease)
        session.add.assert_called_once()
    else:
        assert decisions == []
        session.add.assert_not_called()
        if case != "cleanup_error":
            manager.cleanup_networks.assert_not_awaited()


@pytest.mark.anyio
async def test_startup_refuses_unavailable_lock_without_inspecting_resources(monkeypatch):
    reconciler, manager, session = _reconciler(monkeypatch)
    session.scalar.return_value = False
    report = await reconciler.reconcile_startup()
    assert not report.lock_acquired and report.decisions == ()
    manager.recovery_manifests.assert_not_called()
    manager.cleanup_recovery_manifest.assert_not_awaited()


@pytest.mark.anyio
async def test_unlock_failure_invalidates_database_connection(monkeypatch):
    from unittest.mock import AsyncMock

    import vuzol.execution.reconciliation as module

    reconciler, manager, session = _reconciler(monkeypatch)
    session.scalar.return_value = True
    session.execute.side_effect = RuntimeError("unlock failed")
    manager.recovery_manifests.return_value = ()
    monkeypatch.setattr(module, "release_reaped_account_tickets", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="unlock failed"):
        await reconciler.reconcile_startup()
    connection = session.connection.return_value
    connection.invalidate.assert_awaited_once()
    session.commit.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"account_lease_token": None},
        {"account_lease_token": "bad"},
        {"account_lease_token": str(uuid.UUID(int=1)), "account_id": 17, "account_slot": 0},
        {"account_lease_token": str(uuid.UUID(int=1)), "account_id": "pi", "account_slot": True},
        {"account_lease_token": str(uuid.UUID(int=1)), "account_id": "pi", "account_slot": 100},
        {"account_lease_token": str(uuid.UUID(int=1)), "account_id": "pi", "account_slot": -1},
    ],
)
async def test_malformed_cleanup_tickets_never_release_account_capacity(monkeypatch, metadata):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import vuzol.execution.reconciliation as module

    reconciler, _, session = _reconciler(monkeypatch)
    session.execute.return_value = SimpleNamespace(
        all=lambda: [(uuid.uuid4(), "profile", metadata)]
    )
    release = AsyncMock()
    monkeypatch.setattr(module, "release_account_ticket_after_cleanup", release)
    assert await module.release_reaped_account_tickets(reconciler._factory) is None
    release.assert_not_awaited()


@pytest.mark.anyio
async def test_cleanup_ticket_cursor_advances_only_for_full_batch(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import vuzol.execution.reconciliation as module

    reconciler, _, session = _reconciler(monkeypatch)
    token = uuid.uuid4()
    rows = [
        (
            uuid.uuid4(),
            "profile",
            {"account_id": "pi", "account_slot": 2, "account_lease_token": str(token)},
        )
        for _ in range(100)
    ]
    session.execute.return_value = SimpleNamespace(all=lambda: rows)
    release = AsyncMock()
    monkeypatch.setattr(module, "release_account_ticket_after_cleanup", release)
    cursor = await module.release_reaped_account_tickets(reconciler._factory, uuid.uuid4())
    assert cursor == rows[-1][0]
    assert release.await_count == 100
    release.assert_awaited_with(
        reconciler._factory,
        account_id="pi",
        slot=2,
        token=token,
        account_profile_ids=("profile",),
        cleanup_confirmed=True,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "problem",
    [
        "missing",
        "reaped",
        "no_envelope",
        "bad_envelope",
        "mismatch",
        "networked_without_proxy",
        "proxy_cleanup",
    ],
)
async def test_node_reaper_requires_exact_identity_and_confirmed_proxy_cleanup(
    tmp_path, monkeypatch, problem
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from vuzol.execution import reconciliation as module
    from vuzol.storage.types import ProcessOutcome, ProcessStatus

    from ._execution_helpers import envelope

    configured = envelope(tmp_path)
    process = SimpleNamespace(
        task_id=configured.task_id,
        run_id=configured.run_id,
        step_id=configured.step_id,
        worktree_id=configured.worktree_id,
        lease_generation=configured.lease_generation,
        reaped_at=datetime.now(UTC) if problem == "reaped" else None,
        status=ProcessStatus.RUNNING,
        outcome=None,
        runtime_metadata={
            "node": "remote",
            "runtime_envelope": configured.model_dump(mode="json", exclude={"stdin"}),
        },
    )
    if problem == "no_envelope":
        process.runtime_metadata.pop("runtime_envelope")
    elif problem == "bad_envelope":
        process.runtime_metadata["runtime_envelope"] = {"bad": "envelope"}
    elif problem == "mismatch":
        process.task_id = uuid.uuid4()
    elif problem == "networked_without_proxy":
        process.runtime_metadata["runtime_envelope"]["sandbox"].update(
            network_disabled=False, proxy_network="controlled", https_proxy_url="http://proxy:8080"
        )
    session = AsyncMock()
    session.get.return_value = None if problem == "missing" else process
    factory = MagicMock()
    factory.begin.return_value.__aenter__.return_value = session
    runtime = AsyncMock()
    authority = AsyncMock(
        return_value=_state(
            step_status=StepStatus.COMPLETED, generation=configured.lease_generation
        )
    )
    monkeypatch.setattr(module.ProxyStartupReconciler, "_read_state_for_update", authority)
    proxy = None
    if problem == "proxy_cleanup":
        proxy = MagicMock()
        manifest = ProxyRecoveryManifest(
            Path("/"),
            configured.task_id,
            configured.run_id,
            configured.step_id,
            configured.lease_generation,
            "",
        )
        proxy.recovery_manifests.return_value = (manifest,)
        proxy.cleanup_recovery_manifest = AsyncMock(side_effect=RuntimeError("proxy unavailable"))
    reaper = module.NodeStepReconciler(factory, runtime, proxy, node="remote", owner="executor")
    assert await reaper._reap(uuid.uuid4()) is False
    if problem in {"networked_without_proxy", "proxy_cleanup"}:
        runtime.cleanup.assert_awaited_once()
    else:
        runtime.cleanup.assert_not_awaited()
    if problem != "reaped":
        assert process.reaped_at is None
    if problem == "proxy_cleanup":
        assert process.status is ProcessStatus.UNKNOWN
        assert process.outcome is ProcessOutcome.UNKNOWN
        assert process.runtime_metadata["cleanup_confirmed"] is False
    else:
        assert process.status is ProcessStatus.RUNNING


@pytest.mark.parametrize(
    "key,value",
    [
        ("lease_epoch", True),
        ("outbox_generation", 0),
        ("version", "1"),
        ("fence_epoch", -1),
        ("scope_revision", None),
        ("owner", ""),
        ("conversational", 1),
        ("retrospective", "false"),
        ("job", []),
    ],
)
def test_recovered_turn_rejects_malformed_fencing_before_loading_envelope(key, value):
    from types import SimpleNamespace

    from vuzol.execution.reconciliation import recovered_turn_binding

    context = dict(
        lease_epoch=1,
        outbox_generation=1,
        version=1,
        fence_epoch=1,
        scope_revision=1,
        owner="executor",
        conversational=False,
        retrospective=False,
        job=False,
    )
    context[key] = value
    process = SimpleNamespace(runtime_metadata={"turn_context": context})
    with pytest.raises(ValueError, match=r"malformed recovered|missing turn launch owner"):
        recovered_turn_binding(process, enabled=lambda: True)


@pytest.mark.anyio
@pytest.mark.parametrize("reaped", [False, True])
async def test_turn_reaper_preserves_missing_or_already_reaped_process(monkeypatch, reaped):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from vuzol.execution.reconciliation import TurnStartupReconciler

    proxy_reconciler, _, session = _reconciler(monkeypatch)
    factory = proxy_reconciler._factory
    factory.begin.return_value.__aenter__.return_value = session
    session.get.return_value = SimpleNamespace(reaped_at=NOW) if reaped else None
    runtime = AsyncMock()
    result = await TurnStartupReconciler(
        factory, runtime, owner="executor", enabled=lambda: True
    )._reconcile_one(uuid.uuid4())
    assert result is (
        ReconciliationClassification.ALREADY_ABSENT
        if reaped
        else ReconciliationClassification.PRESERVED_AMBIGUOUS
    )
    runtime.cleanup.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "case",
    [
        "missing_job",
        "foreign_job",
        "old_job",
        "missing_turn",
        "wrong_wakeup",
        "wrong_attempt",
        "active",
        "cleanup_failed",
        "expired",
    ],
)
async def test_job_recovery_requires_durable_goal_turn_and_job_authority(monkeypatch, case):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from vuzol.execution import reconciliation as module
    from vuzol.leadership.persistence import (
        LeadershipInference,
        LeadershipTurn,
        LeadJob,
        ManagedGoal,
    )
    from vuzol.storage.models import SupervisedProcess
    from vuzol.storage.types import ProcessOutcome, ProcessStatus

    proxy_reconciler, _, session = _reconciler(monkeypatch)
    factory = proxy_reconciler._factory
    factory.begin.return_value.__aenter__.return_value = session
    goal_id, job_id, invocation_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    turn_id = uuid.uuid5(job_id, "attempt:3")
    context = SimpleNamespace(
        job=True,
        project_id="project",
        turn_id=turn_id,
        claim=SimpleNamespace(goal_id=goal_id, wakeup_id=job_id, lease_epoch=3),
    )
    process = SimpleNamespace(
        reaped_at=None,
        ended_at=None,
        invocation_id=invocation_id,
        leadership_turn_id=turn_id,
        lease_generation=3,
        profile_id="pi-a",
        status=ProcessStatus.RUNNING,
        outcome=None,
        runtime_metadata={},
    )
    goal = SimpleNamespace(id=goal_id, project_id="project", lease_epoch=3, lifecycle="active")
    turn = SimpleNamespace(id=turn_id, wakeup_id=turn_id, goal_id=goal_id, status="claimed")
    job = SimpleNamespace(goal_id=goal_id, lease_generation=3)
    phase = SimpleNamespace(turn_id=turn_id, profile_id="pi-a")
    if case == "foreign_job":
        job.goal_id = uuid.uuid4()
    elif case == "old_job":
        job.lease_generation = 2
    elif case == "wrong_wakeup":
        turn.wakeup_id = uuid.uuid4()
    elif case == "wrong_attempt":
        turn.id = turn.wakeup_id = uuid.uuid4()
    entities = {
        SupervisedProcess: process,
        ManagedGoal: goal,
        LeadershipTurn: None if case == "missing_turn" else turn,
        LeadJob: None if case == "missing_job" else job,
        LeadershipInference: phase,
    }
    session.get.side_effect = lambda model, _identity, **kwargs: entities[model]
    envelope = object()
    monkeypatch.setattr(
        module, "recovered_turn_binding", lambda *args, **kwargs: (context, envelope)
    )
    current = AsyncMock(return_value=case == "active")
    cleanup = AsyncMock(
        side_effect=RuntimeError("unreachable") if case == "cleanup_failed" else None
    )
    monkeypatch.setattr(module, "invocation_current", current)
    monkeypatch.setattr(module, "cleanup_turn_runtime", cleanup)
    runtime = AsyncMock()
    result = await module.TurnStartupReconciler(
        factory, runtime, owner="executor", enabled=lambda: True
    )._reconcile_one(uuid.uuid4())
    expected = (
        ReconciliationClassification.PRESERVED_ACTIVE_LEASE
        if case == "active"
        else ReconciliationClassification.CLEANUP_FAILED
        if case == "cleanup_failed"
        else ReconciliationClassification.REMOVED_EXPIRED_LEASE
        if case == "expired"
        else ReconciliationClassification.PRESERVED_AMBIGUOUS
    )
    assert result is expected
    assert session.add.call_args.args[0].payload["classification"] == expected.value
    if case in {"cleanup_failed", "expired"}:
        cleanup.assert_awaited_once_with(runtime, None, envelope)
        if case == "expired":
            assert process.reaped_at is not None
            assert process.status is ProcessStatus.EXITED
            assert process.runtime_metadata["cleanup_confirmed"] is True
        else:
            assert process.reaped_at is None
            assert (
                process.status is ProcessStatus.UNKNOWN
                and process.outcome is ProcessOutcome.UNKNOWN
            )
            assert process.runtime_metadata["cleanup_confirmed"] is False
    else:
        cleanup.assert_not_awaited()
        assert process.reaped_at is None
