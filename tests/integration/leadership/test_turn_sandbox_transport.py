"""Real turn ledger, PostgreSQL fencing and Pi adapter; only runtime is fake."""

import json
from pathlib import Path

import pytest
from sqlalchemy import func, select
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    _enable,
    _factory,
    _runtime,
    _session,
)
from tests.unit.execution._execution_helpers import _seccomp_profile
from tests.unit.providers.test_pi_adapter import profile

from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import ExecutionSettings, ProjectLeadSettings
from vuzol.execution.reconciliation import ReconciliationClassification, TurnStartupReconciler
from vuzol.execution.turn_transport import TurnSandboxTransport
from vuzol.leadership.composition import build_project_lead_worker
from vuzol.leadership.persistence import LeadershipInference
from vuzol.providers.account_persistence import ProviderAccountLease
from vuzol.providers.accounts import AccountBoundAdapter
from vuzol.providers.pi import PiCliAdapter
from vuzol.providers.ports import CodexProcessResult
from vuzol.storage.models import SupervisedProcess, Task, UsageRecord, Worktree
from vuzol.storage.types import ProcessOutcome, ProcessStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeRuntime:
    def __init__(self, factory):
        self.factory = factory
        self.calls = []
        self.cleaned = []
        self.cleanup_fails = False

    async def run(self, envelope, cancellation):
        self.calls.append(envelope)
        # This is a separate transaction: intent and ownership must already be
        # committed before a proxy/container may be materialized.
        async with self.factory() as session:
            row = await session.scalar(select(SupervisedProcess))
            phase = await session.get(LeadershipInference, envelope.turn_binding.invocation_id)
            assert row.invocation_id == phase.id and phase.state == "sending"
            assert row.task_id is row.run_id is row.step_id is row.worktree_id is None
            assert row.status is ProcessStatus.RUNNING
            if "account_id" in row.runtime_metadata:
                ticket = await session.scalar(select(ProviderAccountLease))
                assert row.runtime_metadata["account_lease_token"] == str(ticket.token)
        assert envelope.sandbox.empty_workspace
        assert "--tools" in envelope.argv
        assert envelope.argv[envelope.argv.index("--tools") + 1] == "read,grep,find,ls"
        assert all(m.target != Path("/workspace") for m in envelope.sandbox.mounts)
        # Repeated startup while the owner is live must not clean its runtime.
        for _ in range(2):
            report = await TurnStartupReconciler(
                self.factory,
                self,
                owner="other-executor",
                enabled=lambda: True,
            ).reconcile_startup()
            assert (
                report.decisions[0].classification
                is ReconciliationClassification.PRESERVED_ACTIVE_LEASE
            )
        payload = json.dumps({"commands": []})
        message = {
            "role": "assistant",
            "content": [{"type": "text", "text": payload}],
            "stopReason": "stop",
            "usage": {"input": 3, "output": 4},
        }
        stdout = "\n".join(
            json.dumps(event)
            for event in (
                {"type": "agent_end", "messages": [message]},
                {"type": "agent_settled"},
            )
        )
        return CodexProcessResult(0, stdout, "", 1)

    async def cleanup(self, envelope):
        if self.cleanup_fails:
            raise RuntimeError("fake unavailable Docker")
        self.cleaned.append(envelope.turn_binding.invocation_id)


def configuration(tmp_path, *, account_id=None):
    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    (tmp_path / "worktrees").mkdir()
    state = tmp_path / "pi-state"
    state.mkdir()
    configured = profile().model_copy(
        update={
            "roles": frozenset({ProviderRole.LEAD, ProviderRole.REVIEWER}),
            "supported_task_types": frozenset({"project_lead"}),
            "state_directory": state,
            "account_id": account_id,
        }
    )
    runtime = _runtime(tmp_path, repository, lead=True)
    seccomp, digest = _seccomp_profile(tmp_path)
    settings = runtime.settings.model_copy(
        update={
            "project_lead": ProjectLeadSettings(enabled=True, profile_id=configured.id),
            "execution": ExecutionSettings(
                enabled=True,
                require_preflight=False,
                sandbox_seccomp_profile=seccomp,
                sandbox_seccomp_profile_sha256=digest,
            ),
        }
    )
    registries = runtime.registries.model_copy(update={"profiles": ProfileRegistry((configured,))})
    return configured, settings, registries


@pytest.mark.parametrize("account_id", [None, "pi-shared"])
async def test_real_turn_pi_transport_no_workflow_rows_and_durable_cleanup(
    postgres_dsn, tmp_path, account_id, monkeypatch
):
    configured, settings, registries = configuration(tmp_path, account_id=account_id)
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        runtime = FakeRuntime(factory)
        adapter = PiCliAdapter(TurnSandboxTransport(runtime, factory, settings, registries))
        if account_id is not None:
            adapter = AccountBoundAdapter(
                adapter, configured, factory, account_profile_ids=(configured.id,)
            )
        execute = adapter.execute
        failures = []

        async def observed_execute(*args, **kwargs):
            try:
                return await execute(*args, **kwargs)
            except Exception as error:
                failures.append(error)
                raise

        monkeypatch.setattr(adapter, "execute", observed_execute)
        worker = build_project_lead_worker(
            factory,
            settings=settings,
            profiles=registries.profiles,
            owner="turn-transport-test",
            adapter=adapter,
        )
        assert await worker.process_one()
        assert not failures
        assert len(runtime.calls) == 1 and len(runtime.cleaned) == 1
        assert not await worker.process_one()
        async with factory() as session:
            process = await session.scalar(select(SupervisedProcess))
            phase = await session.scalar(select(LeadershipInference))
            assert process.status is ProcessStatus.EXITED
            assert process.outcome is ProcessOutcome.SUCCEEDED and process.reaped_at is not None
            assert phase.state == "received" and phase.result is not None
            assert process.leadership_turn_id == phase.turn_id
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert await session.scalar(select(func.count()).select_from(Worktree)) == 0
            assert await session.scalar(select(func.count()).select_from(UsageRecord)) == 1
            assert await session.scalar(select(func.count()).select_from(ProviderAccountLease)) == 0


@pytest.mark.parametrize("drained", [True, False])
async def test_failed_cleanup_retains_ticket_until_stale_runtime_reaped(
    postgres_dsn, tmp_path, drained
):
    configured, settings, registries = configuration(tmp_path, account_id="pi-shared")
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        runtime = FakeRuntime(factory)
        runtime.cleanup_fails = True
        adapter = AccountBoundAdapter(
            PiCliAdapter(TurnSandboxTransport(runtime, factory, settings, registries)),
            configured,
            factory,
            account_profile_ids=(configured.id,),
        )
        worker = build_project_lead_worker(
            factory,
            settings=settings,
            profiles=registries.profiles,
            owner="turn-transport-test",
            adapter=adapter,
        )
        assert await worker.process_one()
        async with factory() as session:
            process = await session.scalar(select(SupervisedProcess))
            assert process.status is ProcessStatus.UNKNOWN and process.reaped_at is None
            assert await session.scalar(select(func.count()).select_from(ProviderAccountLease)) == 1
        if not drained:
            # Crash before the launcher durably attested that its task drained.
            async with factory.begin() as session:
                process = await session.scalar(select(SupervisedProcess).with_for_update())
                process.runtime_metadata = {
                    key: value
                    for key, value in process.runtime_metadata.items()
                    if key != "launcher_drained"
                }
        reaper = TurnStartupReconciler(
            factory, runtime, owner="restarted", enabled=lambda: settings.project_lead.enabled
        )
        assert (await reaper.reconcile_startup()).decisions[
            0
        ].classification is ReconciliationClassification.CLEANUP_FAILED
        runtime.cleanup_fails = False
        assert (await reaper.reconcile_startup()).removed_count == 1
        assert (await reaper.reconcile_startup()).decisions == ()
        async with factory() as session:
            process = await session.scalar(select(SupervisedProcess))
            assert process.status is ProcessStatus.EXITED and process.reaped_at is not None
            assert process.outcome is ProcessOutcome.UNKNOWN
            assert await session.scalar(select(func.count()).select_from(ProviderAccountLease)) == (
                0 if drained else 1
            )
            assert process.runtime_metadata.get("launcher_drained", False) is drained
        assert len(runtime.calls) == 1
