"""Stage 2: one managed goal through the real queue, with a fake model and transport.

Integration fixtures truncate the selected database. Point
``VUZOL_TEST_DATABASE_DSN`` at a disposable database before running this file.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tests.integration.storage.helpers import storage

from vuzol.cli.applier import applier_handlers
from vuzol.cli.static_publisher_worker import publisher_handlers
from vuzol.config import (
    Capability,
    DeliveryMode,
    GitDeliveryPolicy,
    ProjectConfig,
    RegistryDocument,
    RuntimeConfiguration,
    SandboxProfileConfig,
    Settings,
    TopicConfig,
    TopicKind,
    build_bundle,
)
from vuzol.config.settings import ProjectLeadSettings, get_settings
from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlIngress,
    PackageControlResultCode,
    PackageControlSource,
)
from vuzol.discussion.domain import PackageControlAction, PlanDraft, PlanItemDraft
from vuzol.discussion.sequencer import WorkPackageSequenceConsumer
from vuzol.discussion.service import WorkPackageService
from vuzol.execution.artifact_production import ArtifactProductionHandler
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.git import LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.static_build import StaticBuildHandler
from vuzol.execution.worktrees import WorktreeService
from vuzol.leadership.batches import execute_batch
from vuzol.leadership.commands import LeadCommand
from vuzol.leadership.model import (
    LeadCommandDraft,
    LeadDecision,
    LeadSnapshot,
    UnconfiguredLeadModel,
)
from vuzol.leadership.persistence import GoalDelegation, LeadershipTurn, ManagedGoal
from vuzol.leadership.service import LeadershipCommandService
from vuzol.leadership.wakeups import enqueue_goal_wakeup
from vuzol.leadership.worker import ProjectLeadWorker
from vuzol.projects.dependency_provisioning import DependencyProvisioningHandler
from vuzol.storage.models import Event, PlanRevision, Step, Task, WorkPackage, Worktree
from vuzol.storage.types import PlanRevisionCreatedBy, QueueClass, StepStatus, WorkPackageStatus
from vuzol.storage.unit_of_work import UnitOfWork
from vuzol.telegram.delivery import TelegramDeliveryService
from vuzol.telegram.domain import IngressStatus, MessageUpdate
from vuzol.telegram.ingress import TelegramIngressService
from vuzol.telegram.projections import FakeTelegramClient
from vuzol.workflows.dispatch import WorkflowDispatcher
from vuzol.workflows.domain import OutcomeKind, StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry
from vuzol.workflows.worker import BASE_INTERNAL_HANDLERS, WorkflowWorker

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]

OWNER = 42
CHAT = -100
THREAD = 10
PROJECT = "vuzol"
TEST_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _plan(title: str, summary: str, criteria: list[str], scope: str) -> dict[str, Any]:
    return {
        "title": title,
        "items": [
            {
                "summary": summary,
                "goal": summary,
                "expected_outcome": summary,
                "completion_criteria": criteria,
                "allowed_scope": scope,
            }
        ],
    }


def _decision(*commands: LeadCommandDraft, usage: dict[str, Any] | None = None) -> LeadDecision:
    return LeadDecision(
        commands=commands,
        usage={"cost_known": False} if usage is None else usage,
    )


def _start_decision() -> LeadDecision:
    return _decision(
        LeadCommandDraft(
            name="propose_batch",
            payload={
                "plan": _plan("Package A", "Write the first marker", ["marker-a"], "MARKER.txt")
            },
        ),
        LeadCommandDraft(name="start_batch", payload={}),
        usage={"cost_known": True, "cost_amount": 0},
    )


@asynccontextmanager
async def _factory(postgres_dsn: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine, factory = storage(postgres_dsn)
    try:
        yield factory
    finally:
        await engine.dispose()


def _lead_service(
    factory: async_sessionmaker[AsyncSession],
    *,
    enabled: bool = True,
    repeat_limit: int = 2,
    with_executor: bool = False,
) -> LeadershipCommandService:
    return LeadershipCommandService(
        factory,
        enabled=enabled,
        repeat_limit=repeat_limit,
        default_branches={PROJECT: "main"},
        batch_executor=execute_batch if with_executor else None,
    )


async def _session(factory: async_sessionmaker[AsyncSession], thread_id: int = THREAD) -> uuid.UUID:
    async with UnitOfWork(factory) as uow:
        return await uow.discussions.create_session(
            project_id=PROJECT, chat_id=CHAT, message_thread_id=thread_id
        )


async def _enable(factory: async_sessionmaker[AsyncSession], session_id: uuid.UUID) -> uuid.UUID:
    result = await _lead_service(factory).apply_human(
        __import__("vuzol.workflows.application", fromlist=["Principal"]).Principal(
            OWNER, "telegram"
        ),
        LeadCommand(
            name="enable_managed_goal",
            project_id=PROJECT,
            idempotency_key=f"enable-{session_id.hex[:8]}",
            payload={
                "session_id": str(session_id),
                "integration_branch": f"vuzol-managed/{PROJECT}/{session_id.hex[:12]}",
                "request": "ship the marker",
                "scope": "marker files only",
                "criteria": [{"id": "requested-outcome", "description": "ship the marker"}],
            },
        ),
    )
    assert result.status == "applied"
    assert result.goal_id is not None
    return result.goal_id


def _worker(
    factory: async_sessionmaker[AsyncSession],
    model: Any,
    *,
    enabled: Callable[[], bool] | None = None,
    repeat_limit: int = 2,
    owner: str = "lead-test",
) -> ProjectLeadWorker:
    return ProjectLeadWorker(
        factory,
        owner=owner,
        model=model,
        enabled=enabled or (lambda: True),
        lease_seconds=30,
        repeat_limit=repeat_limit,
        default_branches={PROJECT: "main"},
    )


async def _package_count(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as session:
        return int(await session.scalar(select(func.count()).select_from(WorkPackage)) or 0)


async def test_pause_during_inference_discards_the_response(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class Pausing:
            calls = 0

            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                self.calls += 1
                await _lead_service(factory).apply_human(
                    __import__("vuzol.workflows.application", fromlist=["Principal"]).Principal(
                        OWNER, "telegram"
                    ),
                    LeadCommand(
                        name="pause_goal",
                        project_id=PROJECT,
                        goal_id=snapshot.goal_id,
                        expected_version=snapshot.version,
                        idempotency_key="pause-during-inference",
                        payload={},
                    ),
                )
                return _start_decision()

        worker = _worker(factory, Pausing())
        assert await worker.process_one()
        assert await _package_count(factory) == 0
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            turn = await session.scalar(select(LeadershipTurn))
            assert goal is not None and goal.lifecycle == "paused"
            assert turn is not None and turn.status == "lost"
        assert not await worker.process_one()
        assert await _package_count(factory) == 0


async def test_revoke_during_inference_discards_the_response(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))

        class Revoking:
            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                await _lead_service(factory).apply_human(
                    __import__("vuzol.workflows.application", fromlist=["Principal"]).Principal(
                        OWNER, "telegram"
                    ),
                    LeadCommand(
                        name="revoke_delegation",
                        project_id=PROJECT,
                        goal_id=snapshot.goal_id,
                        expected_version=snapshot.version,
                        idempotency_key="revoke-during-inference",
                        payload={},
                    ),
                )
                return _start_decision()

        worker = _worker(factory, Revoking())
        assert await worker.process_one()
        assert await _package_count(factory) == 0
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            turn = await session.scalar(select(LeadershipTurn))
            assert goal is not None and goal.lifecycle == "paused"
            assert goal.waiting_reason == "delegation_revoked"
            delegation = await session.get(GoalDelegation, goal.delegation_id)
            assert delegation is not None and delegation.revoked_at is not None
            assert turn is not None and turn.status == "lost"
        assert not await worker.process_one()
        assert await _package_count(factory) == 0


async def test_feature_off_before_apply_starts_nothing(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        flag = {"on": True}

        class Switching:
            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                del snapshot
                flag["on"] = False
                return _start_decision()

        worker = _worker(factory, Switching(), enabled=lambda: flag["on"])
        assert await worker.process_one()
        assert await _package_count(factory) == 0
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            turn = await session.scalar(select(LeadershipTurn))
            assert goal is not None and goal.lifecycle == "waiting"
            assert goal.waiting_reason == "feature_disabled"
            assert turn is not None and turn.status == "lost"
            assert turn.usage == {"cost_known": False}
            assert "cost_amount" not in turn.usage


async def test_unconfigured_model_parks_without_a_plan(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        goal_id = await _enable(factory, await _session(factory))
        worker = _worker(factory, UnconfiguredLeadModel())
        assert await worker.process_one()
        async with factory() as session:
            goal = await session.get(ManagedGoal, goal_id)
            turn = await session.scalar(select(LeadershipTurn))
            assert goal is not None and goal.proposed_plan is None
            assert goal.lifecycle == "waiting"
            assert goal.waiting_reason == "lead_model_unconfigured"
            assert turn is not None and turn.usage == {"cost_known": False}
        assert await _package_count(factory) == 0


async def test_crash_before_model_then_applies_once(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))

        class Flaky:
            calls = 0

            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                del snapshot
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("crash-before-call")
                return _start_decision()

        model = Flaky()
        worker = _worker(factory, model)
        with pytest.raises(RuntimeError, match="crash-before-call"):
            await worker.process_one()
        assert await _package_count(factory) == 0
        assert await worker.process_one()
        assert model.calls == 2
        assert await _package_count(factory) == 1
        async with factory() as session:
            turn = await session.scalar(select(LeadershipTurn))
            goal = await session.scalar(select(ManagedGoal))
            assert turn is not None and turn.lease_epoch == 1
            assert goal is not None and goal.lease_epoch == 1
            assert turn.usage == {"cost_known": False}


async def test_crash_after_response_applies_on_resume(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))

        class Once:
            calls = 0

            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                del snapshot
                self.calls += 1
                return _start_decision()

        class CrashApply(ProjectLeadWorker):
            raised = False

            async def _apply_if_current(self, **kwargs: Any) -> None:
                if not self.raised:
                    self.raised = True
                    raise RuntimeError("crash-after-response")
                await super()._apply_if_current(**kwargs)

        model = Once()
        worker = CrashApply(
            factory,
            owner="lead-crash-apply",
            model=model,
            enabled=lambda: True,
            lease_seconds=30,
            default_branches={PROJECT: "main"},
        )
        with pytest.raises(RuntimeError, match="crash-after-response"):
            await worker.process_one()
        assert await _package_count(factory) == 0
        assert await worker.process_one()
        assert model.calls == 1
        assert await _package_count(factory) == 1


async def test_crash_before_ack_retries_the_same_commands(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))

        class Once:
            calls = 0

            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                del snapshot
                self.calls += 1
                return _start_decision()

        class CrashAck(ProjectLeadWorker):
            raised = False

            async def _complete(self, session: AsyncSession, token: Any) -> None:
                if not self.raised:
                    self.raised = True
                    raise RuntimeError("crash-before-ack")
                await super()._complete(session, token)

        model = Once()
        worker = CrashAck(
            factory,
            owner="lead-crash-ack",
            model=model,
            enabled=lambda: True,
            lease_seconds=30,
            default_branches={PROJECT: "main"},
        )
        with pytest.raises(RuntimeError, match="crash-before-ack"):
            await worker.process_one()
        assert await _package_count(factory) == 1
        assert await worker.process_one()
        assert model.calls == 1
        assert await _package_count(factory) == 1
        async with factory() as session:
            turn = await session.scalar(select(LeadershipTurn))
            assert turn is not None and turn.status == "applied"


async def test_second_wakeup_waits_then_sees_the_live_package(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        await _enable(factory, await _session(factory))
        holder: dict[str, Any] = {"factory": factory}

        class Racing:
            calls = 0
            nested = False

            async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
                self.calls += 1
                if self.calls == 1:
                    async with factory.begin() as session:
                        await enqueue_goal_wakeup(
                            session,
                            goal_id=snapshot.goal_id,
                            idempotency_key=f"lead:wakeup:{snapshot.goal_id}:extra",
                            package_id=None,
                            kind="extra",
                        )
                    self.nested = await holder["worker"].process_one()
                return _start_decision()

        model = Racing()
        worker = _worker(factory, model, owner="lead-race")
        holder["worker"] = worker
        assert await worker.process_one()
        assert model.nested is True
        assert await _package_count(factory) == 1
        assert await worker.process_one()
        assert model.calls == 2
        assert await _package_count(factory) == 1
        async with factory() as session:
            from vuzol.leadership.persistence import LeadershipCommandReceipt

            reason = await session.scalar(
                select(LeadershipCommandReceipt.reason).where(
                    LeadershipCommandReceipt.reason == "package_still_live"
                )
            )
            assert reason == "package_still_live"


async def test_repeat_without_new_evidence_asks_the_owner(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        session_id = await _session(factory)
        goal_id = await _enable(factory, session_id)
        service = _lead_service(factory, repeat_limit=1, with_executor=True)
        principal = f"agent:project-lead:{PROJECT}"
        proposed = await service.apply_agent(
            principal,
            "worker",
            LeadCommand(
                name="propose_batch",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=1,
                idempotency_key="propose-repeat",
                payload={
                    "plan": _plan("Package A", "Write the first marker", ["marker-a"], "MARKER.txt")
                },
            ),
        )
        assert proposed.status == "applied"
        started = await service.apply_agent(
            principal,
            "worker",
            LeadCommand(
                name="start_batch",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=proposed.version,
                idempotency_key="start-repeat-1",
                payload={"ignored": True},
            ),
        )
        assert started.status == "applied"
        async with factory() as session:
            package = await session.scalar(select(WorkPackage))
            revision = await session.get(PlanRevision, package.head_revision_id)  # type: ignore[union-attr]
            assert package is not None and revision is not None
            package_id, number, digest, generation = (
                package.id,
                revision.revision_number,
                revision.content_hash,
                package.version,
            )
        async with UnitOfWork(factory) as uow:
            await WorkPackageService(uow).stop_package(
                package_id=package_id,
                revision_number=number,
                h8=digest[:8],
                expected_status_generation=generation,
                user_id=OWNER,
            )
        again = await service.apply_agent(
            principal,
            "worker",
            LeadCommand(
                name="start_batch",
                project_id=PROJECT,
                goal_id=goal_id,
                expected_version=started.version,
                idempotency_key="start-repeat-2",
                payload={},
            ),
        )
        assert again.status == "waiting"
        assert again.reason == "repeat_without_new_evidence"
        assert await _package_count(factory) == 1


async def test_legacy_control_queues_a_package_the_goal_does_not_own(postgres_dsn: str) -> None:
    async with _factory(postgres_dsn) as factory:
        session_id = await _session(factory)
        await _enable(factory, session_id)
        async with UnitOfWork(factory) as uow:
            drafted = await WorkPackageService(uow).create_draft(
                session_id=session_id,
                project_id=PROJECT,
                plan=PlanDraft(
                    title="Human side plan",
                    items=(
                        PlanItemDraft(
                            summary="Do something else",
                            goal="Stay off the managed goal",
                            expected_outcome="Nothing starts",
                            completion_criteria=("side",),
                            allowed_scope="README.md",
                            estimated_complexity=__import__(
                                "vuzol.storage.types", fromlist=["EstimatedComplexity"]
                            ).EstimatedComplexity.SMALL,
                        ),
                    ),
                ),
                created_by=PlanRevisionCreatedBy.USER,
                actor_type="user",
            )
        ingress = PackageControlIngress(
            factory, enabled=True, authorized_user_ids=frozenset({OWNER})
        )
        result = await ingress.apply(
            AuthoritativeControlCommand(
                action=PackageControlAction.APPROVE,
                package_id=drafted.package_id,
                plan_revision_number=drafted.revision_number,
                h8=drafted.content_hash[:8],
                expected_status_generation=drafted.status_generation,
                user_id=OWNER,
                source=PackageControlSource.EXPLICIT_COMMAND,
                external_idempotency_key="legacy-approve-fenced",
            )
        )
        assert result.code is PackageControlResultCode.QUEUED
        async with factory() as session:
            package = await session.get(WorkPackage, drafted.package_id)
            assert package is not None
            assert package.status is WorkPackageStatus.DRAFT


async def test_flag_off_leaves_lead_text_on_the_legacy_path(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    monkeypatch.delenv("VUZOL_PROJECT_LEAD__ENABLED", raising=False)
    get_settings.cache_clear()
    repository = tmp_path / "repositories" / PROJECT
    repository.mkdir(parents=True)
    runtime = _runtime(tmp_path, repository, lead=False, discussion=False)
    async with _factory(postgres_dsn) as factory:
        ingress = TelegramIngressService(runtime, factory)
        result = await ingress.accept_message(
            MessageUpdate(
                bot_id="bot",
                update_id=7,
                chat_id=CHAT,
                message_thread_id=THREAD,
                message_id=7,
                user_id=OWNER,
                text="/lead ship the marker",
            )
        )
        assert result.status is IngressStatus.CREATED
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(ManagedGoal)) == 0
            assert await session.scalar(select(func.count()).select_from(Task)) == 1
    get_settings.cache_clear()


def _runtime(
    tmp_path: Path, repository: Path, *, lead: bool, discussion: bool = True
) -> RuntimeConfiguration:
    settings = Settings(
        environment="test",
        allowed_user_ids=(OWNER,),
        allowed_chat_ids=(CHAT,),
        repository_root=repository.parent,
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        preview_site_root=tmp_path / "preview",
        static_site_root=tmp_path / "published",
        secret_file_root=tmp_path / "secrets",
        project_discussion_enabled=discussion,
        project_lead=ProjectLeadSettings(enabled=lead),
    )
    document = RegistryDocument(
        projects=(
            ProjectConfig(
                id=PROJECT,
                display_name="Fixture",
                repository_path=repository,
                default_branch="main",
                allowed_capabilities=frozenset(Capability),
                sandbox_profile="fixture",
                git_delivery=GitDeliveryPolicy(
                    allowed_modes=frozenset(
                        {DeliveryMode.PATCH, DeliveryMode.APPLY, DeliveryMode.RETAIN}
                    ),
                    approval_required=frozenset({DeliveryMode.APPLY}),
                ),
            ),
        ),
        topics=(
            TopicConfig(
                chat_id=CHAT,
                message_thread_id=THREAD,
                kind=TopicKind.PROJECT,
                project_id=PROJECT,
                default_workflow="coding_task",
            ),
        ),
        sandboxes=(SandboxProfileConfig(id="fixture", image="fixture@sha256:" + "a" * 64),),
    )
    return RuntimeConfiguration(settings=settings, registries=build_bundle(document, settings))


def _git_init(repository: Path) -> str:
    repository.mkdir(parents=True, exist_ok=True)
    (repository / "README.md").write_text("# Before\n")
    for argv in (
        ("init", "-b", "main"),
        ("config", "user.name", "Test"),
        ("config", "user.email", "test@example.invalid"),
        ("add", "README.md"),
        ("commit", "-m", "base"),
    ):
        subprocess.run(("git", "-C", str(repository), *argv), capture_output=True, check=True)
    return subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


class _GoalModel:
    def __init__(self) -> None:
        self.calls = 0

    async def decide(self, snapshot: LeadSnapshot) -> LeadDecision:
        self.calls += 1
        if snapshot.completed_package_count >= 2:
            return _decision(LeadCommandDraft(name="propose_completion", payload={}))
        if snapshot.current_package_status == "completed":
            plan = _plan(
                "Package B",
                "Write the second marker",
                ["requested-outcome"],
                "SECOND.txt",
            )
        else:
            plan = _plan("Package A", "Write the first marker", ["marker-a"], "MARKER.txt")
        return _decision(
            LeadCommandDraft(name="propose_batch", payload={"plan": plan}),
            LeadCommandDraft(name="start_batch", payload={}),
            usage={"cost_known": True, "cost_amount": 0},
        )


async def _drain(workers: tuple[Any, ...], limit: int = 80) -> None:
    for _ in range(limit):
        if not any([await worker.process_one() for worker in workers]):
            return
    raise AssertionError("workflow drain did not settle")


async def test_managed_goal_reviews_repairs_and_resumes_after_restart(
    postgres_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    await run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch, restart_in_subprocess=True)


async def run_managed_goal_flow(
    postgres_dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    request: str = "ship the marker",
    expected_completed: bool = True,
    control_before_apply: str | None = None,
    restart_in_subprocess: bool = False,
    split_corrective_turn: bool = False,
) -> None:
    monkeypatch.delenv("VUZOL_TEST_DATABASE_DSN", raising=False)
    monkeypatch.setenv("VUZOL_PROJECT_LEAD__ENABLED", "true")
    get_settings.cache_clear()
    repository = tmp_path / "repositories" / PROJECT
    base = _git_init(repository)
    runtime = _runtime(tmp_path, repository, lead=True)
    settings = runtime.settings
    trace: dict[str, Any] = {}
    try:
        async with _factory(postgres_dsn) as factory:
            if control_before_apply is not None:
                from tests.integration.leadership.test_lead_review_regressions import control

                from vuzol.execution.result_apply import ResultApplyHandler
                from vuzol.storage.models import Approval
                from vuzol.storage.types import ApprovalStatus

                record = ResultApplyHandler._record_intent
                controlled = False

                async def stop_before_new_effect(handler, request, **kwargs):
                    nonlocal controlled
                    if not controlled:
                        controlled = True
                        async with factory() as session:
                            approved = await session.get(Approval, kwargs["approval_id"])
                            assert approved.status is ApprovalStatus.APPROVED
                            goal = await session.scalar(select(ManagedGoal))
                            goal_id = goal.id
                        result = await control(factory, goal_id, control_before_apply)
                        assert result.status == "applied"
                    return await record(handler, request, **kwargs)

                monkeypatch.setattr(ResultApplyHandler, "_record_intent", stop_before_new_effect)
            client = FakeTelegramClient()
            delivery = TelegramDeliveryService(
                factory,
                client,
                owner="delivery-test",
                lease_seconds=30,
                max_attempts=3,
                retry_min_seconds=0,
                retry_max_seconds=1,
            )
            ingress = TelegramIngressService(runtime, factory)
            opened = await ingress.accept_message(
                MessageUpdate(
                    bot_id="bot",
                    update_id=11,
                    chat_id=CHAT,
                    message_thread_id=THREAD,
                    message_id=11,
                    user_id=OWNER,
                    text=f"/lead {request}",
                )
            )
            assert opened.status is IngressStatus.HANDLED
            for _ in range(5):
                if not await delivery.deliver_one():
                    break
            assert any("Руководитель" in text for _, _, text in client.sent)
            git = LocalGit()
            worktrees = WorktreeService(settings.worktree_root, git, retention_days=1)
            workers = _workflow_workers(runtime, factory, git, worktrees)
            sequence = WorkPackageSequenceConsumer(
                settings, factory, owner="sequence-test", enabled=True
            )
            dispatcher = WorkflowDispatcher(runtime, factory, owner="dispatch-test")
            background = (dispatcher, *workers, sequence)

            first = _provider_worker(factory, settings, owner="lead-1")
            assert await first.process_one()
            await _drain(background)
            if control_before_apply is not None:
                from vuzol.storage.models import Effect
                from vuzol.storage.types import TaskStatus

                async with factory() as session:
                    goal = await session.scalar(select(ManagedGoal))
                    package = await session.scalar(select(WorkPackage))
                    task = await session.scalar(select(Task))
                    assert controlled
                    assert goal.lifecycle in {"paused", "cancelled"}
                    assert package.status is WorkPackageStatus.STOPPED
                    assert task.status is TaskStatus.CANCELLED
                    assert await session.scalar(select(func.count()).select_from(Effect)) == 0
                assert await git.resolve_commit(repository, "main") == base
                assert await git.read_ref(repository, goal.integration_branch) is None
                return
            async with factory() as session:
                first_package = await session.scalar(select(WorkPackage))
                assert first_package is not None
                assert first_package.status is WorkPackageStatus.COMPLETED
                assert first_package.integration_head_commit not in {
                    None,
                    first_package.integration_base_commit,
                }
                trace["package_a"] = str(first_package.id)
                trace["head_a"] = first_package.integration_head_commit
                repairs = list(
                    await session.scalars(select(Step).where(Step.step_type == "execute_code"))
                )
                assert any(
                    isinstance(step.payload, dict) and step.payload.get("repair_for_step_id")
                    for step in repairs
                )
                approved = await session.scalar(
                    select(func.count())
                    .select_from(Event)
                    .where(Event.event_type == "result.auto_approved_by_managed_integration")
                )
                assert approved == 1

            assert await git.resolve_commit(repository, "main") == base
            resumed = _provider_worker(factory, settings, owner="lead-2")
            if restart_in_subprocess:
                restarted = await asyncio.to_thread(
                    subprocess.run,
                    [
                        sys.executable,
                        "-c",
                        _RESTART_SCRIPT,
                        postgres_dsn,
                        str(tmp_path),
                        str(repository),
                    ],
                    cwd=TEST_REPOSITORY_ROOT,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                assert restarted.returncode == 0, restarted.stdout + restarted.stderr
                child = next(
                    json.loads(line.removeprefix("LEAD_RESTART "))
                    for line in restarted.stdout.splitlines()
                    if line.startswith("LEAD_RESTART ")
                )
                assert child["pid"] != os.getpid()
                assert child["processed"] is True
                trace["restart"] = "separate Python process; fresh composition and DB engine"
            else:
                assert await resumed.process_one()
            if split_corrective_turn:
                assert await _package_count(factory) == 1
                resumed = _provider_worker(factory, settings, owner="lead-2-continuation")
                assert await resumed.process_one()
            assert await _package_count(factory) == 2
            assert not await resumed.process_one()
            assert await _package_count(factory) == 2
            await _drain(background)
            closer = _provider_worker(factory, settings, owner="lead-3")
            for _ in range(3):
                async with factory() as session:
                    goal = await session.scalar(select(ManagedGoal))
                    if goal is not None and goal.lifecycle == "completed":
                        break
                if not await closer.process_one():
                    break
            async with factory() as session:
                goal = await session.scalar(select(ManagedGoal))
                packages = list(
                    await session.scalars(select(WorkPackage).order_by(WorkPackage.created_at))
                )
                assert goal is not None
                assert [package.status for package in packages] == [
                    WorkPackageStatus.COMPLETED,
                    WorkPackageStatus.COMPLETED,
                ]
                assert all("status" not in item for item in goal.criteria)
                assert goal.criteria[0]["id"] == "requested-outcome"
                turns = list(await session.scalars(select(LeadershipTurn)))
                assert turns
                assert all(turn.usage.get("cost_known") is False for turn in turns)
                assert all("cost_amount" not in turn.usage for turn in turns)
                trace["goal"] = str(goal.id)
                trace["branch"] = goal.integration_branch
                trace["packages"] = [str(package.id) for package in packages]
                trace["lifecycle"] = goal.lifecycle
            if expected_completed:
                assert trace["lifecycle"] == "completed"
            else:
                assert trace["lifecycle"] != "completed"
                return
            managed = await git.resolve_commit(repository, trace["branch"])
            assert managed != base
            assert await git.resolve_commit(repository, "main") == base
            shown = await asyncio.to_thread(
                subprocess.run,
                ("git", "-C", str(repository), "show", f"{managed}:SECOND.txt"),
                capture_output=True,
                text=True,
                check=True,
            )
            marker = await asyncio.to_thread(
                subprocess.run,
                ("git", "-C", str(repository), "show", f"{managed}:MARKER.txt"),
                capture_output=True,
                text=True,
                check=True,
            )
            assert "marker-b" in shown.stdout
            assert "repaired" in marker.stdout
            trace["head_b"] = managed
            trace["main"] = base
            for _ in range(300):
                if not await delivery.deliver_one():
                    break
            assert any(
                "✅ <b>ГОТОВО · Руководитель</b>" in text
                and "Подтверждено критериев: 1 из 1" in text
                for _, _, text in client.sent
            )
            print("LEAD_TRACE")
            for key, value in trace.items():
                print(f"{key}={value}")
    finally:
        get_settings.cache_clear()


_RESTART_SCRIPT = """
import asyncio
import json
import os
import sys
from pathlib import Path
from tests.integration.leadership.test_managed_goal_flow import (
    _factory, _provider_worker, _runtime,
)

async def main():
    dsn, root, repository = sys.argv[1:]
    runtime = _runtime(Path(root), Path(repository), lead=True)
    async with _factory(dsn) as factory:
        worker = _provider_worker(factory, runtime.settings, owner="lead-restarted-process")
        processed = await worker.process_one()
    print("LEAD_RESTART " + json.dumps({"pid": os.getpid(), "processed": processed}))

asyncio.run(main())
"""


def _provider_worker(factory, settings, *, owner, transport=None):
    from tests.integration.leadership.test_lead_surface import _profile

    from vuzol.config.models import ProviderRole
    from vuzol.config.registries import ProfileRegistry
    from vuzol.leadership.composition import build_project_lead_worker
    from vuzol.providers.domain import NormalizedUsage, ProviderResult, ProviderResultStatus

    class Transport:
        async def execute(self, request, profile, cancellation):
            del profile, cancellation
            assert request.timeout_seconds == 180
            prompt = request.original_input
            if request.role is ProviderRole.REVIEWER:
                body = json.loads(prompt.split("\n", 1)[1])
                files = body["files"]
                passed = (
                    body["request"] == "ship the marker"
                    and files.get("MARKER.txt") == "marker-a\nrepaired\n"
                    and files.get("SECOND.txt") == "marker-b\n"
                )
                payload = {
                    "result_commit": body["result_commit"],
                    "criteria": [
                        {
                            "criterion_id": item["id"],
                            "verdict": "pass" if passed else "insufficient",
                            "explanation": "Both committed markers have the required contents."
                            if passed
                            else "These marker files do not implement the requested outcome.",
                            "evidence_paths": ["MARKER.txt", "SECOND.txt"] if passed else [],
                        }
                        for item in body["criteria"]
                    ],
                }
            elif "completed_packages: 2" in prompt:
                payload = {"commands": [{"name": "propose_completion", "payload": {}}]}
            else:
                second = "package: completed" in prompt
                plan = _plan(
                    "Package B" if second else "Package A",
                    "Write the second marker" if second else "Write the first marker",
                    ["requested-outcome"] if second else ["marker-a"],
                    "SECOND.txt" if second else "MARKER.txt",
                )
                payload = {
                    "commands": [
                        {"name": "propose_batch", "payload": {"plan": plan}},
                        {"name": "start_batch", "payload": {}},
                    ]
                }
            return ProviderResult(
                status=ProviderResultStatus.SUCCEEDED,
                structured_output=payload,
                usage=NormalizedUsage(duration_ms=1),
                adapter_version="fixture-provider",
            )

        async def health(self, profile):
            from vuzol.providers.domain import EffectiveProfileState

            return EffectiveProfileState()

    profiles = ProfileRegistry(
        (
            _profile().model_copy(update={"context_limit": 16000}),
            _profile().model_copy(
                update={
                    "id": "goal-review",
                    "roles": frozenset({ProviderRole.REVIEWER}),
                    "context_limit": 16000,
                }
            ),
        )
    )
    configured = settings.model_copy(
        update={
            "project_lead": settings.project_lead.model_copy(
                update={
                    "profile_id": "lead-local",
                    "reviewer_profile_id": "goal-review",
                    "inference_timeout_seconds": 180,
                }
            )
        }
    )
    return build_project_lead_worker(
        factory,
        settings=configured,
        profiles=profiles,
        owner=owner,
        default_branches={PROJECT: "main"},
        adapter=transport if transport is not None else Transport(),
        reviewer_adapter=transport if transport is not None else Transport(),
    )


def _workflow_workers(
    runtime: RuntimeConfiguration,
    factory: async_sessionmaker[AsyncSession],
    git: LocalGit,
    worktrees: WorktreeService,
) -> tuple[WorkflowWorker, WorkflowWorker, WorkflowWorker, WorkflowWorker]:
    settings = runtime.settings

    class Execute:
        async def execute(
            self, request: StepExecutionRequest, cancellation: CancellationContext
        ) -> StepOutcome:
            assert not cancellation.requested
            async with factory.begin() as session:
                row = await session.scalar(
                    select(Worktree).where(Worktree.run_id == request.run_id).with_for_update()
                )
                assert row is not None
                path = Path(row.path)
                marker = path / "MARKER.txt"
                if request.payload.get("repair_for_step_id"):
                    repaired = marker.read_text(encoding="utf-8") + "repaired\n"
                    marker.write_text(repaired, encoding="utf-8")
                    await git.stage_paths(path, ("MARKER.txt",))
                elif not marker.exists():
                    marker.write_text("marker-a\n", encoding="utf-8")
                    await git.stage_paths(path, ("MARKER.txt",))
                else:
                    (path / "SECOND.txt").write_text("marker-b\n", encoding="utf-8")
                    await git.stage_paths(path, ("SECOND.txt",))
                await git.create_commit(path, "managed goal edit")
                await worktrees.retain(session, worktree_id=row.id)
            return StepOutcome.succeeded({"implementation_summary": "Wrote the marker."})

    class Validate:
        async def execute(
            self, request: StepExecutionRequest, cancellation: CancellationContext
        ) -> StepOutcome:
            del cancellation
            async with factory() as session:
                row = await session.scalar(
                    select(Worktree).where(Worktree.run_id == request.run_id)
                )
                assert row is not None and row.result_commit and row.base_commit
            return StepOutcome.succeeded(
                {
                    "structured_output": {
                        "result_commit": row.result_commit,
                        "base_commit": row.base_commit,
                        "gates": [{"name": "fixture", "exit_code": 0}],
                        "changed_files": ["MARKER.txt"],
                    }
                }
            )

    class Review:
        async def execute(
            self, request: StepExecutionRequest, cancellation: CancellationContext
        ) -> StepOutcome:
            del cancellation
            async with factory() as session:
                row = await session.scalar(
                    select(Worktree).where(Worktree.run_id == request.run_id)
                )
                completed = await session.scalar(
                    select(func.count())
                    .select_from(Step)
                    .where(
                        Step.run_id == request.run_id,
                        Step.step_type == "execute_code",
                        Step.status == StepStatus.COMPLETED,
                    )
                )
                assert row is not None and row.result_commit and row.diff_hash
                marker = Path(row.path) / "MARKER.txt"
                text = marker.read_text(encoding="utf-8") if marker.exists() else ""
            if int(completed or 0) <= 1 and "repaired" not in text:
                return StepOutcome(
                    kind=OutcomeKind.BLOCKED,
                    result={"summary": "marker needs a repair line"},
                    category="review_changes_requested",
                    summary="marker needs a repair line",
                    unknown_effects=False,
                )
            return StepOutcome.succeeded(
                {
                    "structured_output": {
                        "schema_version": "review.fixture",
                        "verdict": "pass",
                        "review_kind": "fixture",
                        "risk": "low",
                        "base_commit": row.base_commit,
                        "result_commit": row.result_commit,
                        "diff_hash": row.diff_hash,
                        "findings": [],
                    }
                }
            )

    artifacts = ArtifactStore(settings.artifact_root, max_bytes=1_000_000, retention_days=1)
    internal = WorkflowWorker(
        settings,
        factory,
        owner="common",
        handlers={**BASE_INTERNAL_HANDLERS, "review": Review()},
    )
    heavy = WorkflowWorker(
        settings,
        factory,
        owner="executor",
        queue_classes=frozenset({QueueClass.HEAVY}),
        handlers={
            "prepare_worktree": PrepareWorktreeHandler(
                factory, runtime.registries, worktrees, owner="fixture", settings=settings
            ),
            "execute_code": Execute(),
            "ensure_dependencies": DependencyProvisioningHandler(
                factory,
                settings.dependency_provisioning,
                None,
                worktree_root=settings.worktree_root,
            ),
            "validate": Validate(),
            "produce_artifacts": ArtifactProductionHandler(
                factory,
                runtime.registries,
                git,
                _magic(),
                _magic(),
                artifacts,
                worktree_root=settings.worktree_root,
            ),
            "build_static": StaticBuildHandler(
                factory,
                runtime.registries,
                git,
                _magic(),
                _magic(),
                worktree_root=settings.worktree_root,
            ),
        },
    )
    applier = WorkflowWorker(
        settings,
        factory,
        owner="applier",
        handlers=applier_handlers(factory, runtime),
        capabilities=frozenset({Capability.GIT, Capability.HOST_ADMIN}),
        queue_classes=frozenset({QueueClass.PRIVILEGED}),
    )
    registry = PreviewRuntimeRegistry()
    publisher = WorkflowWorker(
        settings,
        factory,
        owner="publisher",
        handlers=publisher_handlers(factory, runtime, registry),
        capabilities=frozenset({Capability.FILESYSTEM_WRITE}),
        queue_classes=frozenset({QueueClass.LIGHT}),
    )
    return internal, heavy, applier, publisher


def _magic() -> Any:
    from unittest.mock import MagicMock

    return MagicMock()
