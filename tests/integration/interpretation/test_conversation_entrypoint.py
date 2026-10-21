"""Real executor/worker and Git; fake Docker/ACL, Jev HTTP and Telegram transport."""

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
from tests.integration.telegram.test_delivery import service
from vuzol.cli import executor, worker
from vuzol.config import RegistryDocument, RuntimeConfiguration, build_bundle
from vuzol.config.models import ProviderRole
from vuzol.execution.access import WorktreeAccessManager
from vuzol.execution.domain import MountMode
from vuzol.leadership.persistence import ManagedGoal
from vuzol.providers.ports import CodexProcessResult
from vuzol.storage.models import RoutingDecision, Step, SupervisedProcess, Task, UsageRecord
from vuzol.storage.types import ProcessStatus, StepStatus, TaskStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("mode", ["lead", "think"])
@pytest.mark.parametrize(
    "model,effort",
    [
        ("vendor/current-lead", None),
        ("muse-spark-1.3-contributor", "high"),
    ],
)
def test_lead_conversation_through_executor_entrypoint(
    postgres_dsn, tmp_path, monkeypatch, mode, model, effort
):
    async def scenario():
        runtime = modes_runtime(tmp_path)
        # Same deployed task-type shape, arbitrary model selection; do not edit
        # supported_task_types to make the provider boundary test pass.
        chosen = runtime.registries.profiles.get("lead-only")
        assert "architecture" not in chosen.supported_task_types
        chosen = chosen.model_copy(
            update={
                "model": model,
                "model_reasoning_effort": effort,
                "roles": chosen.roles | ({ProviderRole.EXECUTOR} if mode == "think" else set()),
            }
        )
        configured = runtime.settings.model_copy(
            update={
                "chat": runtime.settings.chat.model_copy(update={"profile_id": chosen.id})
                if mode == "think"
                else runtime.settings.chat,
            }
        )
        document = RegistryDocument(
            projects=runtime.registries.projects.items(),
            topics=runtime.registries.topics.items(),
            profiles=tuple(
                chosen if p.id == chosen.id else p for p in runtime.registries.profiles.items()
            ),
            sandboxes=runtime.registries.sandboxes.items(),
        )
        runtime = RuntimeConfiguration(
            settings=configured, registries=build_bundle(document, configured)
        )
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
                assert envelope.argv[envelope.argv.index("--model") + 1] == model
                if effort is not None:
                    assert envelope.argv[envelope.argv.index("--thinking") + 1] == effort
                mounts = {mount.purpose: mount for mount in envelope.sandbox.mounts}
                assert mounts["task-worktree"].mode is MountMode.READ_ONLY
                assert mounts["worktree-git-metadata"].mode is MountMode.READ_ONLY
                assert envelope.sandbox.network_disabled
                assert json.loads(envelope.stdin)["role"] == (
                    "lead" if mode == "lead" else "executor"
                )
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
        monkeypatch.setattr(worker, "get_runtime_configuration", lambda **kwargs: runtime)
        monkeypatch.setattr(worker, "resolve_database_dsn", lambda _: SecretStr(postgres_dsn))

        async def deliver_and_finalize(*, worker, **kwargs):
            assert await worker.process_one()  # deliver_discussion_reply
            assert await worker.process_one()  # finalize
            assert not await worker.process_one()

        monkeypatch.setattr(worker, "run_runtime", deliver_and_finalize)
        engine, factory = storage(postgres_dsn)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn(f"/{mode}"))
            await ingress.accept_message(turn("Let's discuss the plan; do not execute it.", 1102))
            # This newer mode must not substitute the queued answer's speaker.
            await ingress.accept_message(turn("/think" if mode == "lead" else "/lead", 1103))
            assert await classify(runtime, factory, "plan")
            await executor.run()
            await worker.run()  # real internal handler persists reply and completes the task
            telegram = FakeTelegramClient()
            delivery = service(factory, telegram)
            for _ in range(8):
                if not await delivery.deliver_one():
                    break
            else:
                raise AssertionError("conversation delivery did not drain")
            speaker = "Руководитель" if mode == "lead" else "Обсуждение"
            assert any(
                speaker in str(call)
                and model in str(call)
                and "Discussing, not executing." in str(call)
                for call in telegram.sent
            )
            async with factory() as session:
                step = await session.scalar(select(Step).where(Step.step_type == "execute_agent"))
                assert step.status is StepStatus.COMPLETED, (
                    step.failure_category,
                    step.failure_summary,
                )
                assert step.executor_profile_id == "lead-only"
                assert step.payload["project_model_slot"] == mode
                decision = await session.scalar(
                    select(RoutingDecision).where(RoutingDecision.step_id == step.id)
                )
                assert decision.inputs["task_type"] == "discussion_agent_internal"
                process = (await session.scalars(select(SupervisedProcess))).one()
                assert process.status is ProcessStatus.EXITED
                assert process.runtime_metadata["cleanup_confirmed"] is True
                usage = await session.scalar(
                    select(UsageRecord).where(UsageRecord.step_id == step.id)
                )
                assert usage.profile_id == "lead-only"
                assert await session.scalar(select(func.count()).select_from(Task)) == 1
                task = (await session.scalars(select(Task))).one()
                assert task.status is TaskStatus.COMPLETED
                assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
                assert len(launched) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())
