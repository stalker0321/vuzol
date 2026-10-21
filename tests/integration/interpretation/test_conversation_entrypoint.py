"""Real executor composition and local Git, fake Docker/ACL and model HTTP only."""

import asyncio
import hashlib
import json
import subprocess
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from tests.integration.interpretation.test_conversation_modes import classify, modes_runtime, turn
from tests.integration.storage.helpers import storage
from vuzol.cli import executor
from vuzol.execution.access import WorktreeAccessManager
from vuzol.execution.domain import MountMode
from vuzol.leadership.persistence import ManagedGoal
from vuzol.providers.ports import CodexProcessResult
from vuzol.storage.models import Step, SupervisedProcess, Task, UsageRecord
from vuzol.storage.types import ProcessStatus, StepStatus
from vuzol.telegram.ingress import TelegramIngressService

pytestmark = pytest.mark.postgresql


def test_lead_conversation_through_executor_entrypoint(postgres_dsn, tmp_path, monkeypatch):
    async def scenario():
        runtime = modes_runtime(tmp_path)
        repository = runtime.registries.projects.get("vuzol").repository_path
        for args in (
            ("init", "-b", "main"),
            (
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--allow-empty",
                "-m",
                "base",
            ),
        ):
            await asyncio.to_thread(
                subprocess.run,
                ("git", "-C", str(repository), *args),
                check=True,
                capture_output=True,
            )
        for profile in runtime.registries.profiles.items():
            if profile.state_directory is not None:
                profile.state_directory.mkdir(parents=True)
        seccomp = tmp_path / "seccomp.json"
        seccomp.write_text("{}")
        seccomp.chmod(0o600)
        settings = runtime.settings.model_copy(
            update={
                "worktree_root": tmp_path / "worktrees",
                "execution": runtime.settings.execution.model_copy(
                    update={
                        "enabled": True,
                        "require_preflight": False,
                        "sandbox_seccomp_profile": seccomp,
                        "sandbox_seccomp_profile_sha256": hashlib.sha256(b"{}").hexdigest(),
                        "reviewer_profile_id": "lead-only",
                    }
                ),
                "dependency_provisioning": runtime.settings.dependency_provisioning.model_copy(
                    update={"enabled": False, "environment_root": tmp_path / "dependencies"}
                ),
            }
        )
        runtime = runtime.model_copy(update={"settings": settings})
        launched = []

        class DockerBoundary:
            async def run(self, envelope, cancellation):
                assert envelope.turn_binding is None and not cancellation.requested
                assert envelope.profile_id == "lead-only"
                assert envelope.argv[envelope.argv.index("--tools") + 1] == "read,grep,find,ls"
                assert envelope.argv[envelope.argv.index("--model") + 1] == "vendor/current-lead"
                mounts = {mount.purpose: mount for mount in envelope.sandbox.mounts}
                assert mounts["task-worktree"].mode is MountMode.READ_ONLY
                assert mounts["worktree-git-metadata"].mode is MountMode.READ_ONLY
                assert envelope.sandbox.network_disabled
                assert json.loads(envelope.stdin)["role"] == "lead"
                launched.append(envelope)
                message = {
                    "role": "assistant",
                    "stopReason": "stop",
                    "content": [{"type": "text", "text": "Discussing, not executing."}],
                    "usage": {"input": 100, "output": 10},
                }
                return CodexProcessResult(
                    0,
                    json.dumps({"type": "agent_end", "messages": [message]})
                    + '\n{"type":"agent_settled"}\n',
                    "",
                    1,
                )

        monkeypatch.setattr(WorktreeAccessManager, "preflight", AsyncMock())
        monkeypatch.setattr(executor, "get_runtime_configuration", lambda **kwargs: runtime)
        monkeypatch.setattr(executor, "resolve_database_dsn", lambda _: SecretStr(postgres_dsn))
        monkeypatch.setattr(executor, "RootlessDockerRuntime", lambda _: DockerBoundary())
        monkeypatch.setattr(executor, "_refresh_subscription_limits_tick", AsyncMock())

        async def two_iterations(processor, *args, **kwargs):
            assert await processor.process_one()  # real prepare_worktree
            assert await processor.process_one()  # real routing/handler/adapter/envelope

        monkeypatch.setattr(executor, "_run_loop", two_iterations)
        engine, factory = storage(postgres_dsn)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn("/lead"))
            await ingress.accept_message(turn("Let's discuss the plan; do not execute it.", 1102))
            assert await classify(runtime, factory, "plan")
            await executor.run()
            async with factory() as session:
                step = await session.scalar(select(Step).where(Step.step_type == "execute_agent"))
                assert step.status is StepStatus.COMPLETED, (
                    step.failure_category,
                    step.failure_summary,
                )
                assert step.executor_profile_id == "lead-only"
                assert step.payload["project_model_slot"] == "lead"
                process = (await session.scalars(select(SupervisedProcess))).one()
                assert process.status is ProcessStatus.EXITED
                assert process.runtime_metadata["cleanup_confirmed"] is True
                usage = await session.scalar(
                    select(UsageRecord).where(UsageRecord.step_id == step.id)
                )
                assert usage.profile_id == "lead-only"
                assert await session.scalar(select(func.count()).select_from(Task)) == 1
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
                assert len(launched) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())
