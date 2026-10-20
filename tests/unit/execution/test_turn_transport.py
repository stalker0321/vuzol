"""Real CLI envelopes and transport, with fake PostgreSQL/runtime boundaries."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from vuzol.config.models import ProviderRole, SandboxProfileConfig
from vuzol.config.revision import content_revision
from vuzol.execution import reconciliation, turn_transport
from vuzol.execution.domain import ProcessEnvelope, TurnProcessBinding
from vuzol.execution.reconciliation import ReconciliationClassification, TurnStartupReconciler
from vuzol.execution.sandbox import docker_run_argv
from vuzol.execution.turn_transport import TurnSandboxTransport, turn_runtime_lock
from vuzol.leadership.fencing import TurnClaim
from vuzol.leadership.invocation_context import TurnInvocationContext, current_turn
from vuzol.leadership.persistence import LeadershipInference, LeadershipTurn, ManagedGoal
from vuzol.providers.accounts import AccountExecution
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.kimi import KIMI_MODEL, KimiCliAdapter
from vuzol.providers.pi import PiCliAdapter
from vuzol.providers.ports import CodexProcessResult
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import SupervisedProcess, TransactionalOutbox
from vuzol.storage.types import ProcessOutcome, ProcessStatus
from vuzol.workflows.ports import CancellationContext

from ..providers.test_pi_adapter import FIXTURES, profile, request
from ._execution_helpers import _seccomp_profile

pytestmark = pytest.mark.anyio


class Captured(Exception):
    pass


class Capture:
    invocation = None

    async def run(self, invocation, cancellation):
        self.invocation = invocation
        raise Captured


class Store:
    def __init__(self, phase):
        self.phase = phase
        self.process = None
        self.committed = False
        self.entities = {}
        self.connection = AsyncMock()
        self.connection.invalidated = False
        self.connection.scalar.return_value = True

    def __call__(self):
        return Session(self)

    def begin(self):
        return Session(self)


class Session:
    def __init__(self, store):
        self.store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *error):
        self.store.committed = True

    async def connection(self):
        return self.store.connection

    async def get(self, model, identity, **kwargs):
        if model is LeadershipInference:
            return self.store.phase
        if model is SupervisedProcess:
            return self.store.process
        return self.store.entities.get(model)

    async def scalar(self, query):
        return None if self.store.process is None else self.store.process.id

    def add(self, value):
        if isinstance(value, SupervisedProcess):
            value.id = uuid.uuid4()
            self.store.process = value

    async def flush(self):
        pass

    async def refresh(self, *args, **kwargs):
        pass


class Runtime:
    def __init__(self, store):
        self.store = store
        self.calls = []
        self.cleanup = AsyncMock()
        self.exit_code = 0

    async def run(self, envelope, cancellation):
        assert self.store.committed
        assert self.store.process.invocation_id == envelope.turn_binding.invocation_id
        self.calls.append(envelope)
        return CodexProcessResult(
            self.exit_code, (FIXTURES / "print-json-text.jsonl").read_text(), "", 2
        )


@pytest.fixture
def setup(tmp_path, monkeypatch):
    context = TurnInvocationContext(
        TurnClaim(uuid.uuid4(), uuid.uuid4(), "executor", 3, 5), uuid.uuid4(), "project", 1, 2, 1
    )
    invocation_id = uuid.uuid4()
    configured = profile().model_copy(
        update={
            "roles": frozenset({ProviderRole.LEAD, ProviderRole.REVIEWER}),
            "state_directory": tmp_path,
        }
    )
    req = request(
        task_id=invocation_id,
        run_id=invocation_id,
        step_id=invocation_id,
        role=ProviderRole.LEAD,
        required_capabilities=frozenset(),
        sandbox_reference=f"lead-turn:{context.turn_id}",
        lease_generation=3,
    )
    phase = SimpleNamespace(
        id=invocation_id,
        turn_id=context.turn_id,
        state="sending",
        phase="lead",
        profile_id=configured.id,
        profile_revision=content_revision(configured),
        request=req.model_dump(mode="json"),
    )
    store = Store(phase)
    runtime = Runtime(store)
    sandbox = SandboxProfileConfig(id="test", image="test@sha256:" + "a" * 64)
    project = SimpleNamespace(
        enabled=True, sandbox_profile="test", network=configured.runtime_network
    )
    registries = SimpleNamespace(
        profiles=SimpleNamespace(get=lambda key: configured),
        projects=SimpleNamespace(get=lambda key: project),
        sandboxes=SimpleNamespace(get=lambda key: sandbox),
    )
    seccomp_path, digest = _seccomp_profile(tmp_path)
    settings = SimpleNamespace(
        execution=SimpleNamespace(
            sandbox_seccomp_profile=seccomp_path,
            sandbox_seccomp_profile_sha256=digest,
        )
    )
    transport = TurnSandboxTransport(runtime, store, settings, registries)
    monkeypatch.setattr(turn_transport, "invocation_current", AsyncMock(return_value=True))
    token = current_turn.set(context)
    yield SimpleNamespace(
        context=context,
        profile=configured,
        request=req,
        store=store,
        runtime=runtime,
        transport=transport,
        registries=registries,
        phase=phase,
    )
    current_turn.reset(token)


@pytest.mark.parametrize("role", [ProviderRole.LEAD, ProviderRole.REVIEWER])
@pytest.mark.parametrize(
    "provider,adapter",
    [
        ("codex", CodexCliAdapter),
        ("grok", GrokCliAdapter),
        ("kimi", KimiCliAdapter),
        ("pi", PiCliAdapter),
    ],
)
async def test_all_canonical_role_payloads_use_honest_step_less_binding(
    setup, role, provider, adapter
):
    configured = setup.profile.model_copy(
        update={
            "provider": provider,
            "model": KIMI_MODEL if provider == "kimi" else setup.profile.model,
        }
    )
    setup.registries.profiles.get = lambda key: configured
    setup.phase.profile_revision = content_revision(configured)
    setup.phase.phase = "lead" if role is ProviderRole.LEAD else "goal_review"
    req = setup.request.model_copy(update={"role": role})
    setup.phase.request = req.model_dump(mode="json")
    capture = Capture()
    with pytest.raises(Captured):
        await adapter(capture).execute(req, configured, CancellationContext())
    await setup.transport.run(capture.invocation, CancellationContext())
    process = setup.store.process
    assert process.status is ProcessStatus.EXITED
    assert process.outcome is ProcessOutcome.SUCCEEDED
    assert process.reaped_at is not None
    assert all(
        getattr(process, name) is None for name in ("task_id", "run_id", "step_id", "worktree_id")
    )
    envelope = setup.runtime.calls[0]
    assert envelope.sandbox.empty_workspace
    assert all(m.target != Path("/workspace") for m in envelope.sandbox.mounts)
    assert "vuzol.task_id" not in envelope.ownership_labels
    argv = docker_run_argv(Path("/unused/docker.sock"), envelope.container_name, envelope)
    assert "/workspace:ro,nosuid,nodev,noexec,size=1048576,mode=0555" in argv
    with pytest.raises(ValueError, match="already exists"):
        await setup.transport.run(capture.invocation, CancellationContext())
    assert len(setup.runtime.calls) == 1


async def test_real_pi_adapter_success_with_unshared_profile(setup):
    result = await PiCliAdapter(setup.transport).execute(
        setup.request, setup.profile, CancellationContext()
    )
    assert result.text == "surface probe reply"
    assert "account_id" not in setup.store.process.runtime_metadata


async def invocation(setup):
    capture = Capture()
    with pytest.raises(Captured):
        await PiCliAdapter(capture).execute(setup.request, setup.profile, CancellationContext())
    return capture.invocation


async def test_explicit_account_requires_matching_ticket_and_persists_before_launch(
    setup, monkeypatch
):
    configured = setup.profile.model_copy(update={"account_id": "shared"})
    setup.registries.profiles.get = lambda key: configured
    setup.phase.profile_revision = content_revision(configured)
    call = await invocation(setup)
    with pytest.raises(ValueError, match="account admission"):
        await setup.transport.run(call, CancellationContext())
    assert setup.store.process is None
    account = AccountExecution("shared", 0, uuid.uuid4())
    monkeypatch.setattr(turn_transport, "current_account_execution", lambda: account)
    await setup.transport.run(call, CancellationContext())
    assert setup.store.process.runtime_metadata["account_lease_token"] == str(account.token)


async def test_stale_authority_and_changed_evidence_fail_before_resources(setup, monkeypatch):
    call = await invocation(setup)
    monkeypatch.setattr(turn_transport, "invocation_current", AsyncMock(return_value=False))
    with pytest.raises(LeaseLost):
        await setup.transport.run(call, CancellationContext())
    monkeypatch.setattr(turn_transport, "invocation_current", AsyncMock(return_value=True))
    setup.phase.request["task_draft"] = {"untrusted": "different"}
    with pytest.raises(ValueError, match="durable inference input"):
        await setup.transport.run(call, CancellationContext())
    assert setup.store.process is None and not setup.runtime.calls


async def test_failure_outcome_and_failed_cleanup_are_distinct(setup):
    setup.runtime.exit_code = 2
    await setup.transport.run(await invocation(setup), CancellationContext())
    assert setup.store.process.outcome is ProcessOutcome.FAILED
    setup.store.process = None
    setup.runtime.cleanup.side_effect = RuntimeError("foreign container")
    with pytest.raises(RuntimeError, match="foreign"):
        await setup.transport.run(await invocation(setup), CancellationContext())
    assert setup.store.process.status is ProcessStatus.UNKNOWN
    assert setup.store.process.reaped_at is None
    assert not setup.store.process.runtime_metadata["cleanup_confirmed"]
    assert setup.store.process.runtime_metadata["launcher_drained"] is True


async def test_recovery_preserves_active_then_reaps_when_feature_disabled(setup, monkeypatch):
    _, _, process_id = await setup.transport._prepare(await invocation(setup), setup.context)
    context = setup.context
    setup.store.entities = {
        ManagedGoal: SimpleNamespace(
            id=context.claim.goal_id,
            project_id=context.project_id,
            lease_epoch=3,
            lifecycle="active",
        ),
        LeadershipTurn: SimpleNamespace(
            id=context.turn_id,
            goal_id=context.claim.goal_id,
            wakeup_id=context.claim.wakeup_id,
            status="claimed",
        ),
        TransactionalOutbox: SimpleNamespace(lease_generation=5),
    }

    async def is_current(session, recovered):
        return recovered.enabled()

    monkeypatch.setattr(reconciliation, "invocation_current", is_current)
    enabled = True
    reaper = TurnStartupReconciler(
        setup.store, setup.runtime, owner="new-executor", enabled=lambda: enabled
    )
    for _ in range(2):
        assert (
            await reaper._reconcile_one(process_id)
            is ReconciliationClassification.PRESERVED_ACTIVE_LEASE
        )
    setup.runtime.cleanup.assert_not_called()
    enabled = False
    assert (
        await reaper._reconcile_one(process_id)
        is ReconciliationClassification.REMOVED_TERMINAL_LEFTOVER
    )
    setup.runtime.cleanup.assert_awaited_once()
    assert setup.store.process.reaped_at is not None
    assert setup.store.process.outcome is ProcessOutcome.UNKNOWN
    assert "launcher_drained" not in setup.store.process.runtime_metadata


async def test_lock_lost_commit_ack_invalidates_connection(setup):
    setup.store.connection.commit.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        async with turn_runtime_lock(setup.store, setup.phase.id):
            pytest.fail("must not enter after lost acquisition acknowledgement")
    setup.store.connection.invalidate.assert_awaited_once()


async def test_unshared_watchdog_bounds_database_hang(setup, monkeypatch):
    async def hang(*args):
        await asyncio.Event().wait()

    setup.store.connection.execute.side_effect = hang
    monkeypatch.setattr(turn_transport, "TURN_DB_TIMEOUT_SECONDS", 0.01)
    cancellation = CancellationContext()
    with pytest.raises(TimeoutError):
        await setup.transport._watch(setup.context, cancellation, setup.store.connection)
    assert cancellation.requested


async def test_ready_receipt_survives_same_tick_revocation(setup, monkeypatch):
    monkeypatch.setattr(setup.transport, "_watch", AsyncMock(side_effect=LeaseLost("revoked")))
    result = await setup.transport.run(await invocation(setup), CancellationContext())
    assert result.exit_code == 0
    assert setup.store.process.outcome is ProcessOutcome.SUCCEEDED
    setup.runtime.cleanup.assert_awaited_once()


async def test_repeated_cancel_cannot_attest_drain_before_runtime_finishes(setup, monkeypatch):
    started, stopping, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def run(envelope, cancellation):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopping.set()
            await stopped.wait()

    monkeypatch.setattr(setup.runtime, "run", run)
    task = asyncio.create_task(setup.transport.run(await invocation(setup), CancellationContext()))
    await started.wait()
    task.cancel()
    await stopping.wait()
    task.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert "launcher_drained" not in setup.store.process.runtime_metadata
    setup.runtime.cleanup.assert_not_awaited()
    stopped.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert setup.store.process.runtime_metadata["launcher_drained"] is True
    setup.runtime.cleanup.assert_awaited_once()


async def test_reaper_rotates_bounded_batches_past_active_oldest(monkeypatch):
    now = datetime.now(UTC)
    first = [(uuid.uuid4(), uuid.uuid4(), 1, now) for _ in range(100)]
    last = [(uuid.uuid4(), uuid.uuid4(), 1, now)]
    batches = iter((first, last))
    queries = []

    class QuerySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, query):
            queries.append(query)
            return SimpleNamespace(all=lambda: next(batches))

    @asynccontextmanager
    async def owned(*args):
        yield None

    monkeypatch.setattr(reconciliation, "turn_runtime_lock", owned)
    monkeypatch.setattr(
        reconciliation, "release_reaped_account_tickets", AsyncMock(return_value=None)
    )
    reaper = TurnStartupReconciler(QuerySession, None, owner="test", enabled=lambda: True)
    assert len((await reaper.reconcile_startup()).decisions) == 100
    assert reaper._cursor == (now, first[-1][0])
    assert (await reaper.reconcile_startup()).decisions[0].step_id == str(last[0][1])
    assert reaper._cursor is None
    rendered = [
        str(q.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        for q in queries
    ]
    assert all("LIMIT 100" in query for query in rendered)
    assert "(supervised_processes.created_at, supervised_processes.id) >" in rendered[1]


async def test_ticket_release_query_requires_drain_proof_and_extant_token():
    queries = []

    class QuerySession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, query):
            queries.append(query)
            return SimpleNamespace(all=lambda: [])

    assert await reconciliation.release_reaped_account_tickets(QuerySession) is None
    sql = str(
        queries[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert "launcher_drained" in sql and "IS true" in sql
    assert "JOIN provider_account_leases" in sql and "account_lease_token" in sql
    assert "LIMIT 100" in sql


async def test_turn_binding_cannot_mix_workflow_ids(setup):
    binding = TurnProcessBinding(turn_id=setup.context.turn_id, invocation_id=setup.phase.id)
    assert binding.resource_name(1, "sandbox") != binding.resource_name(2, "sandbox")
    envelope, _, _ = await setup.transport._prepare(await invocation(setup), setup.context)
    with pytest.raises(ValueError, match="cannot carry workflow"):
        ProcessEnvelope.model_validate({**envelope.model_dump(), "task_id": uuid.uuid4()})
