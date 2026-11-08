"""A service asks, the executor runs the CLI model in its sandbox; only the runtime is fake."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from pydantic import BaseModel
from sqlalchemy import select

from tests.integration.leadership.test_managed_goal_flow import _factory, _runtime
from tests.unit.execution._execution_helpers import _seccomp_profile
from tests.unit.providers.test_pi_adapter import profile
from vuzol.config.models import NetworkPolicy, SandboxNetworkMode
from vuzol.config.registries import ProfileRegistry, SandboxRegistry
from vuzol.config.settings import ExecutionSettings
from vuzol.inbox.model import InboxModelError
from vuzol.model_calls.client import RelayedModel
from vuzol.model_calls.queue import (
    ModelCallClaim,
    claim_call,
    current_model_call,
    finish_call,
    submit_call,
)
from vuzol.model_calls.transport import ModelCallSandboxTransport
from vuzol.model_calls.worker import ModelCallReconciler, ModelCallWorker
from vuzol.providers.pi import PiCliAdapter
from vuzol.providers.ports import CodexInvocation, CodexProcessResult
from vuzol.providers.role_cli import RoleBoundCliAdapter
from vuzol.storage.models import ModelCall, SupervisedProcess
from vuzol.storage.types import ProcessStatus
from vuzol.workflows.ports import CancellationContext

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


class Answer(BaseModel):
    reply: str


class FakeRuntime:
    def __init__(self, factory, answer="готово"):
        self.factory = factory
        self.answer = answer
        self.envelopes = []

    async def run(self, envelope, cancellation):
        self.envelopes.append(envelope)
        async with self.factory() as session:
            process = await session.scalar(select(SupervisedProcess))
            assert process.model_call_id == envelope.turn_binding.turn_id
            assert process.task_id is process.leadership_turn_id is None
            assert process.status is ProcessStatus.RUNNING
        assert envelope.sandbox.empty_workspace and not envelope.sandbox.network_disabled
        assert [m.purpose for m in envelope.sandbox.mounts] == ["provider-state"]
        assert "--no-tools" in envelope.argv
        message = {
            "role": "assistant",
            "content": [{"type": "text", "text": json.dumps({"reply": self.answer})}],
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


class FakeProxy:
    def __init__(self):
        self.cleaned = []
        self.cleanup_fails = False

    async def create(self, *_args, **_kwargs):
        return SimpleNamespace(
            networks=SimpleNamespace(internal_name="model-call-net"),
            proxy_url="http://vuzol-proxy:8888",
        )

    async def wait_until_dead(self, _lease):
        await asyncio.Event().wait()

    async def cleanup_turn(self, binding, _generation):
        if self.cleanup_fails:
            raise RuntimeError("fake unavailable Docker")
        self.cleaned.append(binding.invocation_id)


def configuration(tmp_path):
    repository = tmp_path / "repositories" / "demo"
    repository.mkdir(parents=True)
    state = tmp_path / "pi-state"
    state.mkdir()
    configured = profile().model_copy(
        update={
            "state_directory": state,
            "runtime_network": NetworkPolicy(
                enabled=True,
                destinations=({"url": "https://opencode.ai", "purpose": "inference"},),
            ),
        }
    )
    runtime = _runtime(tmp_path, repository, lead=True)
    seccomp, digest = _seccomp_profile(tmp_path)
    settings = runtime.settings.model_copy(
        update={
            "execution": ExecutionSettings(
                enabled=True,
                require_preflight=False,
                sandbox_seccomp_profile=seccomp,
                sandbox_seccomp_profile_sha256=digest,
                model_call_sandbox_profile="model-calls",
            ),
        }
    )
    base = runtime.registries.sandboxes.items()[0]
    sandbox = base.model_copy(
        update={"id": "model-calls", "network_mode": SandboxNetworkMode.HTTPS_PROXY}
    )
    registries = runtime.registries.model_copy(
        update={
            "profiles": ProfileRegistry((configured,)),
            "sandboxes": SandboxRegistry((base, sandbox)),
        }
    )
    return configured, settings, registries


def worker_for(factory, settings, registries, runtime, proxy):
    transport = ModelCallSandboxTransport(runtime, factory, settings, registries, proxy)
    unused = PiCliAdapter(transport)  # the workflow paths are never taken here
    adapter = RoleBoundCliAdapter(unused, unused, unused, PiCliAdapter(transport))
    return ModelCallWorker(factory, registries.profiles, lambda _id: adapter, owner="executor-1")


async def serve(worker):
    """Run the worker once the caller's call is queued."""
    for _ in range(200):
        if await worker.process_one():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("the call never reached the queue")


async def test_service_asks_and_the_executor_answers_in_a_sandbox(postgres_dsn, tmp_path):
    configured, settings, registries = configuration(tmp_path)
    async with _factory(postgres_dsn) as factory:
        runtime, proxy = FakeRuntime(factory), FakeProxy()
        worker = worker_for(factory, settings, registries, runtime, proxy)
        model = RelayedModel(
            factory,
            profile_id=configured.id,
            model=configured.model,
            purpose="assistant_turn",
            timeout_seconds=60,
            poll_seconds=0.05,
        )
        asked = asyncio.create_task(model.ask(Answer, system="Отвечай коротко.", user="x" * 50_000))
        await serve(worker)
        assert (await asyncio.wait_for(asked, 10)).reply == "готово"
        assert current_model_call.get() is None
        (envelope,) = runtime.envelopes
        body = json.loads(envelope.stdin)
        assert body["original_input"] == "Отвечай коротко."
        # The input is split into context items, never cut.
        assert "".join(item["content"] for item in body["context"]) == "x" * 50_000
        async with factory() as session:
            call = await session.scalar(select(ModelCall))
            process = await session.scalar(select(SupervisedProcess))
        assert call.state == "succeeded" and call.usage["profile_id"] == configured.id
        assert process.reaped_at is not None and process.runtime_metadata["launcher_drained"]
        assert proxy.cleaned == [envelope.turn_binding.invocation_id]


async def test_a_failed_call_reaches_the_caller_so_it_can_fall_back(postgres_dsn, tmp_path):
    configured, settings, registries = configuration(tmp_path)
    async with _factory(postgres_dsn) as factory:
        runtime = FakeRuntime(factory, answer="x")
        runtime.run = None  # the sandbox cannot start at all
        worker = worker_for(factory, settings, registries, runtime, FakeProxy())
        model = RelayedModel(
            factory,
            profile_id=configured.id,
            model=configured.model,
            purpose="assistant_turn",
            timeout_seconds=60,
            poll_seconds=0.05,
        )
        asked = asyncio.create_task(model.ask(Answer, system="s", user="u"))
        await serve(worker)
        with pytest.raises(InboxModelError):
            await asyncio.wait_for(asked, 10)
        async with factory() as session:
            call = await session.scalar(select(ModelCall))
        assert call.state == "failed" and call.error


@pytest.mark.parametrize("allowed", [False, True])
async def test_only_mcp_servers_the_executor_allows_are_run(postgres_dsn, tmp_path, allowed):
    configured, settings, registries = configuration(tmp_path)
    if allowed:
        settings = settings.model_copy(
            update={
                "execution": settings.execution.model_copy(
                    update={"model_call_mcp_servers": {"kiwi": "https://mcp.kiwi.com"}}
                )
            }
        )
    async with _factory(postgres_dsn) as factory:
        runtime = FakeRuntime(factory)
        worker = worker_for(factory, settings, registries, runtime, FakeProxy())
        async with factory.begin() as session:
            await submit_call(
                session,
                purpose="p",
                profile_id=configured.id,
                system="s",
                user="u",
                output_schema={"type": "object"},
                timeout_seconds=60,
                wait_seconds=600,
                mcp_servers={"kiwi": "https://mcp.kiwi.com"},
            )
        assert await worker.process_one()
        async with factory() as session:
            call = await session.scalar(select(ModelCall))
        assert (call.state == "succeeded") is allowed
        assert len(runtime.envelopes) == (1 if allowed else 0)


async def test_the_sandbox_runs_only_for_the_current_lease(postgres_dsn, tmp_path):
    configured, settings, registries = configuration(tmp_path)
    async with _factory(postgres_dsn) as factory:
        transport = ModelCallSandboxTransport(
            FakeRuntime(factory), factory, settings, registries, FakeProxy()
        )
        invocation = CodexInvocation(
            argv=("pi",),
            stdin="{}",
            runtime_identity="vuzol-pi-a",
            state_directory=str(configured.state_directory),
            timeout_seconds=60,
            sandbox_reference="model-call:00000000-0000-0000-0000-000000000000",
        )
        with pytest.raises(ValueError, match="model call binding"):
            await transport.run(invocation, CancellationContext())


async def test_lease_deadline_and_an_executor_that_died(postgres_dsn):
    async with _factory(postgres_dsn) as factory:
        async with factory.begin() as session:
            late = await submit_call(
                session,
                purpose="p",
                profile_id="any",
                system="s",
                user="u",
                output_schema={},
                timeout_seconds=60,
                wait_seconds=60,
            )
            (await session.get(ModelCall, late)).deadline_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
            alive = await submit_call(
                session,
                purpose="p",
                profile_id="any",
                system="s",
                user="u",
                output_schema={},
                timeout_seconds=60,
                wait_seconds=600,
            )
        async with factory.begin() as session:
            first = await claim_call(session, owner="dead")
        assert first == ModelCallClaim(alive, "dead", 1)
        async with factory() as session:
            assert (await session.get(ModelCall, late)).error == "deadline_passed"
        async with factory.begin() as session:
            assert await claim_call(session, owner="other") is None
            (await session.get(ModelCall, alive)).lease_expires_at = datetime.now(UTC)
        async with factory.begin() as session:
            second = await claim_call(session, owner="other")
        assert second == ModelCallClaim(alive, "other", 2)
        async with factory.begin() as session:
            # The dead executor's late answer must not overwrite the new attempt.
            assert not await finish_call(session, first, output={"reply": "old"})
            assert await finish_call(session, second, output={"reply": "new"})
        async with factory() as session:
            assert (await session.get(ModelCall, alive)).output == {"reply": "new"}


async def test_a_sandbox_left_behind_is_reaped_once_the_lease_is_gone(postgres_dsn, tmp_path):
    configured, settings, registries = configuration(tmp_path)
    async with _factory(postgres_dsn) as factory:
        runtime, proxy = FakeRuntime(factory), FakeProxy()
        proxy.cleanup_fails = True
        worker = worker_for(factory, settings, registries, runtime, proxy)
        async with factory.begin() as session:
            await submit_call(
                session,
                purpose="p",
                profile_id=configured.id,
                system="s",
                user="u",
                output_schema={"type": "object"},
                timeout_seconds=60,
                wait_seconds=600,
            )
        assert await worker.process_one()
        async with factory() as session:
            process = await session.scalar(select(SupervisedProcess))
        assert process.reaped_at is None and process.status is ProcessStatus.UNKNOWN
        proxy.cleanup_fails = False
        reaper = ModelCallReconciler(factory, runtime, proxy)
        assert await reaper.reconcile() == 1
        assert await reaper.reconcile() == 0
        async with factory() as session:
            process = await session.scalar(select(SupervisedProcess))
        assert process.reaped_at is not None
