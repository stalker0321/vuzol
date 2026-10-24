"""Real executor leadership turn; fake Docker/ACL and Telegram transport."""

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
from vuzol.cli import executor
from vuzol.config import RegistryDocument, RuntimeConfiguration, build_bundle
from vuzol.config.models import ProviderRole
from vuzol.execution.access import WorktreeAccessManager
from vuzol.leadership.persistence import LeadershipInference, LeadershipTurn, ManagedGoal
from vuzol.providers.ports import CodexProcessResult
from vuzol.storage.models import Step, SupervisedProcess, Task, UsageRecord
from vuzol.storage.types import ProcessStatus
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient

pytestmark = pytest.mark.postgresql


# /think talks to its own discussion model; this covers the lead entrypoint.
@pytest.mark.parametrize("mode", ["lead"])
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
                assert envelope.turn_binding is not None and not cancellation.requested
                assert envelope.profile_id == "lead-only"
                assert "--no-tools" in envelope.argv and "--tools" not in envelope.argv
                assert envelope.task_id is None and envelope.run_id is None
                assert envelope.argv[envelope.argv.index("--model") + 1] == model
                if effort is not None:
                    assert envelope.argv[envelope.argv.index("--thinking") + 1] == effort
                assert [mount.purpose for mount in envelope.sandbox.mounts] == ["provider-state"]
                assert envelope.sandbox.empty_workspace and envelope.sandbox.network_disabled
                body = json.loads(envelope.stdin)
                assert body["role"] == "lead"
                assert f'"mode": "{mode}"' in body["original_input"]
                launched.append(envelope)
                message = {
                    "role": "assistant",
                    "stopReason": "stop",
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {"commands": [], "reply": "Обсудим варианты без запуска работы."}
                            ),
                        }
                    ],
                    "usage": {"input": 100, "output": 10},
                }
                return CodexProcessResult(
                    0,
                    json.dumps({"type": "agent_end", "messages": [message]})
                    + '\n{"type":"agent_settled"}\n',
                    "",
                    1,
                )

            async def cleanup(self, envelope):
                assert envelope in launched

        monkeypatch.setattr(WorktreeAccessManager, "preflight", AsyncMock())
        monkeypatch.setattr(executor, "get_runtime_configuration", lambda **kwargs: runtime)
        monkeypatch.setattr(executor, "resolve_database_dsn", lambda _: SecretStr(postgres_dsn))
        monkeypatch.setattr(executor, "RootlessDockerRuntime", lambda *_a, **_k: DockerBoundary())
        monkeypatch.setattr(executor, "_refresh_subscription_limits_tick", AsyncMock())

        async def one_turn(processor, *args, **kwargs):
            assert processor._lead is not None
            assert await processor.process_one()

        monkeypatch.setattr(executor, "_run_loop", one_turn)
        engine, factory = storage(postgres_dsn)
        try:
            ingress = TelegramIngressService(runtime, factory)
            await ingress.accept_message(turn(f"/{mode}"))
            await ingress.accept_message(turn("Let's discuss the plan; do not execute it.", 1102))
            # This newer mode must not substitute the queued answer's speaker.
            await ingress.accept_message(turn("/think" if mode == "lead" else "/lead", 1103))
            assert not await classify(runtime, factory, "plan")
            await executor.run()  # turn adapter persists the reply without workflow steps
            telegram = FakeTelegramClient()
            delivery = service(factory, telegram)
            for _ in range(8):
                if not await delivery.deliver_one():
                    break
            else:
                raise AssertionError("conversation delivery did not drain")
            assert (
                sum("Обсудим варианты без запуска работы." in text for _, _, text in telegram.sent)
                == 1
            )
            async with factory() as session:
                assert await session.scalar(select(func.count()).select_from(Step)) == 0
                assert await session.scalar(select(func.count()).select_from(Task)) == 0
                saved = (await session.scalars(select(LeadershipTurn))).one()
                assert saved.model_assignments["lead"]["model"] == model
                assert saved.model_assignments["lead"]["reasoning_effort"] == effort
                process = (await session.scalars(select(SupervisedProcess))).one()
                assert process.status is ProcessStatus.EXITED and process.reaped_at is not None
                phase = (await session.scalars(select(LeadershipInference))).one()
                usage = await session.scalar(
                    select(UsageRecord).where(UsageRecord.invocation_id == phase.id)
                )
                assert usage.profile_id == "lead-only" and usage.task_id is None
                assert (await session.scalar(select(ManagedGoal))).lifecycle == "draft"
                assert len(launched) == 1
        finally:
            await engine.dispose()

    asyncio.run(scenario())
