"""Actual host entrypoint, Pi adapter and durable supervisor; fake Docker I/O only."""

import hashlib
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select
from tests.integration.leadership.test_managed_goal_flow import (
    PROJECT,
    _enable,
    _factory,
    _runtime,
    _session,
)
from tests.unit.providers.test_pi_adapter import profile as pi_profile

from vuzol.cli import executor
from vuzol.config.models import ProviderRole
from vuzol.config.registries import ProfileRegistry
from vuzol.config.settings import ProjectLeadSettings
from vuzol.leadership.persistence import LeadershipInference, ManagedGoal
from vuzol.providers.account_persistence import ProviderAccountLease
from vuzol.providers.ports import CodexProcessResult
from vuzol.storage.models import SupervisedProcess, Task, UsageRecord
from vuzol.storage.types import ProcessOutcome, ProcessStatus

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.parametrize("reasoning_effort", [None, "high"])
async def test_executor_runs_selected_pi_lead_without_creating_a_fake_task(
    postgres_dsn,
    tmp_path,
    monkeypatch,
    reasoning_effort,
):
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    state = tmp_path / "provider-state"
    state.mkdir()
    seccomp = tmp_path / "seccomp.json"
    seccomp.write_text("{}")
    seccomp.chmod(0o600)
    runtime = _runtime(tmp_path, repository, lead=True)
    profile = pi_profile().model_copy(
        update={
            "id": "any-pi-model",
            "model": "arbitrary-model-not-hardcoded",
            "model_reasoning_effort": reasoning_effort,
            "state_directory": state,
            "account_id": "fixture-account-c",
            "roles": frozenset({ProviderRole.LEAD, ProviderRole.REVIEWER, ProviderRole.EXECUTOR}),
            "context_limit": 32_000,
        }
    )
    runtime = runtime.model_copy(
        update={
            "settings": runtime.settings.model_copy(
                update={
                    "project_lead": ProjectLeadSettings(
                        enabled=True, profile_id=profile.id, inference_timeout_seconds=180
                    ),
                    "execution": runtime.settings.execution.model_copy(
                        update={
                            "enabled": True,
                            "require_preflight": False,
                            "sandbox_seccomp_profile": seccomp,
                            "sandbox_seccomp_profile_sha256": hashlib.sha256(b"{}").hexdigest(),
                        }
                    ),
                }
            ),
            "registries": runtime.registries.model_copy(
                update={"profiles": ProfileRegistry((profile,))}
            ),
        }
    )
    invocations, cleaned = [], []

    class DockerBoundary:
        async def run(self, envelope, cancellation):
            assert not cancellation.requested
            assert envelope.turn_binding is not None
            assert (
                envelope.task_id
                is envelope.run_id
                is envelope.step_id
                is envelope.worktree_id
                is None
            )
            assert "vuzol.task_id" not in envelope.ownership_labels
            assert envelope.sandbox.empty_workspace
            assert envelope.sandbox.timeout_seconds == 180
            assert envelope.argv[envelope.argv.index("--model") + 1] == profile.model
            if reasoning_effort is None:
                assert "--thinking" not in envelope.argv
            else:
                assert envelope.argv[envelope.argv.index("--thinking") + 1] == reasoning_effort
            assert "--no-tools" in envelope.argv and "--tools" not in envelope.argv
            assert "--no-context-files" in envelope.argv
            assert "--system-prompt" in envelope.argv
            assert [mount.purpose for mount in envelope.sandbox.mounts] == ["provider-state"]
            body = json.loads(envelope.stdin)
            assert body["role"] == "lead"
            assert "owner_user_id" not in envelope.stdin
            invocations.append(envelope)
            message = {
                "role": "assistant",
                "content": [{"type": "text", "text": '{"commands":[]}'}],
                "stopReason": "stop",
                "responseId": "local-receipt",
                "usage": {"input": 50, "output": 10, "cost": {"total": 0.01}},
            }
            return CodexProcessResult(
                exit_code=0,
                stderr="",
                duration_ms=12,
                stdout=json.dumps({"type": "agent_end", "messages": [message], "willRetry": False})
                + '\n{"type":"agent_settled"}\n',
            )

        async def cleanup(self, envelope):
            cleaned.append(envelope.container_name)

    class AccessBoundary:
        def __init__(self, *args):
            pass

        async def preflight(self, identities):
            pass

    monkeypatch.setattr(executor, "get_runtime_configuration", lambda **kwargs: runtime)
    monkeypatch.setattr(executor, "resolve_database_dsn", lambda settings: SecretStr(postgres_dsn))
    monkeypatch.setattr(executor, "RootlessDockerRuntime", lambda socket: DockerBoundary())
    monkeypatch.setattr(executor, "WorktreeAccessManager", AccessBoundary)
    monkeypatch.setattr(executor, "_refresh_subscription_limits_tick", AsyncMock())

    async def one_turn(processor, *args, **kwargs):
        assert "plan" in processor._providers._handlers
        assert processor._reviews is not None
        assert processor._lead is not None
        assert await processor.process_one()

    monkeypatch.setattr(executor, "_run_loop", one_turn)
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        await executor.run()
        assert len(invocations) == len(cleaned) == 1
        async with factory() as session:
            process = await session.scalar(select(SupervisedProcess))
            phase = await session.scalar(select(LeadershipInference))
            goal = await session.scalar(select(ManagedGoal))
            usage = await session.scalar(select(UsageRecord))
            assert goal.waiting_reason == "model_idle"
            assert process.status is ProcessStatus.EXITED
            assert process.outcome is ProcessOutcome.SUCCEEDED
            assert process.reaped_at is not None
            assert process.invocation_id == phase.id == usage.invocation_id
            assert phase.state == "received"
            assert process.runtime_metadata["account_id"] == "fixture-account-c"
            assert await session.scalar(select(func.count()).select_from(Task)) == 0
            assert await session.scalar(select(func.count()).select_from(ProviderAccountLease)) == 0
