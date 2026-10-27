"""Fixed change/report chains and stored coding-v3 with real lifecycle handlers.

No production DB, model calls, Telegram writes or external publication. The
fixture is deliberately docs-only: trusted sandbox gates have separate tests.
"""

# ruff: noqa: ASYNC240 -- bounded local test fixtures.

import asyncio
import subprocess
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy import func, select

from tests.integration.storage.helpers import storage
from tests.integration.workflows._test_runtime_helpers import planned_coding_draft, seed_interpreted
from tests.integration.workflows.stored_workflows import stored_workflow
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
    build_bundle,
)
from vuzol.execution.artifact_production import ArtifactProductionHandler
from vuzol.execution.artifacts import ArtifactStore
from vuzol.execution.git import LocalGit
from vuzol.execution.handlers import PrepareWorktreeHandler
from vuzol.execution.result_validation import ResultValidationHandler
from vuzol.execution.static_build import StaticBuildHandler
from vuzol.execution.worktrees import WorktreeService
from vuzol.interpretation.domain import TaskOperation, TaskType
from vuzol.projects.dependency_provisioning import DependencyProvisioningHandler
from vuzol.review import ResultReviewHandler
from vuzol.review.domain import ReviewVerdict, ReviewVerdictKind
from vuzol.storage.models import (
    Approval,
    ProjectEnvironmentRevision,
    Run,
    Step,
    Task,
    TransactionalOutbox,
    Worktree,
)
from vuzol.storage.types import (
    ApprovalStatus,
    QueueClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
)
from vuzol.telegram.projections import build_status_card, step_status_label
from vuzol.workflows.application import Principal, apply_task_command
from vuzol.workflows.compiler import compile_workflow
from vuzol.workflows.controls import decide_result
from vuzol.workflows.domain import StepOutcome
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.runtime_preview import PreviewRuntimeRegistry
from vuzol.workflows.service import materialize_run
from vuzol.workflows.worker import BASE_INTERNAL_HANDLERS, WorkflowWorker

pytestmark = pytest.mark.postgresql


@pytest.mark.parametrize("kind", ["fixed", "coding.v3", "report"])
def test_real_handlers_wait_for_approval_then_finish(
    postgres_dsn: str, tmp_path: Path, kind: str
) -> None:
    repository = tmp_path / "repositories" / "vuzol"
    repository.mkdir(parents=True)
    (repository / "README.md").write_text("# Before\n")
    for argv in (
        ("init", "-b", "main"),
        ("config", "user.name", "Test"),
        ("config", "user.email", "test@example.invalid"),
        ("add", "README.md"),
        ("commit", "-m", "base"),
    ):
        subprocess.run(("git", "-C", str(repository), *argv), capture_output=True, check=True)
    base = subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    settings = Settings(
        environment="test",
        repository_root=repository.parent,
        worktree_root=tmp_path / "worktrees",
        artifact_root=tmp_path / "artifacts",
        preview_site_root=tmp_path / "preview",
        static_site_root=tmp_path / "published",
    )
    project = ProjectConfig(
        id="vuzol",
        display_name="Fixture",
        repository_path=repository,
        default_branch="main",
        allowed_capabilities=frozenset(Capability),
        sandbox_profile="fixture",
        git_delivery=GitDeliveryPolicy(
            allowed_modes=frozenset({DeliveryMode.PATCH, DeliveryMode.APPLY, DeliveryMode.RETAIN}),
            approval_required=frozenset({DeliveryMode.APPLY}),
        ),
    )
    runtime = RuntimeConfiguration(
        settings=settings,
        registries=build_bundle(
            RegistryDocument(
                projects=(project,),
                sandboxes=(
                    SandboxProfileConfig(
                        id="fixture",
                        image="fixture@sha256:" + "a" * 64,
                    ),
                ),
            ),
            settings,
        ),
    )

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        git = LocalGit()
        worktrees = WorktreeService(settings.worktree_root, git, retention_days=1)
        draft = planned_coding_draft().model_copy(
            update={
                "needs_planning": False,
                "needs_review": True,
                "suggested_risk": RiskLevel.LOW,
            }
        )
        if kind == "report":
            draft = draft.model_copy(
                update={"task_type": TaskType.RESEARCH, "operation": TaskOperation.INSPECT}
            )
        task_id, interpretation_id = await seed_interpreted(factory, draft)
        async with factory.begin() as session:
            task = await session.get(Task, task_id)
            assert task is not None
            task.task_type = draft.task_type.value
            task.project_id = "vuzol"
            task.source_thread_id = 10
            task.risk = RiskLevel.LOW
            run = await materialize_run(
                session,
                task_id=task_id,
                workflow=(
                    stored_workflow(
                        draft, interpretation_id=interpretation_id, configured_workflow=kind
                    )
                    if kind == "coding.v3"
                    else compile_workflow(draft, interpretation_id=interpretation_id)
                ),
                configuration_revision=runtime.registries.revision,
                policy_revision="b" * 64,
                prompt_revision="fixture",
                automatic_start=False,
            )
            run_id = run.id
            card = await build_status_card(session, task_id)
            assert "Этап: Ожидает запуска" in card.html
            assert "Завершение" not in card.html
            assert card.buttons == ("start",)
            result = await apply_task_command(
                session, task_id=task_id, command="start", principal=Principal(1, "telegram")
            )
            assert result.applied
            await session.flush()
            projections = await session.scalar(
                select(func.count()).select_from(TransactionalOutbox)
            )
            again = await apply_task_command(
                session, task_id=task_id, command="start", principal=Principal(1, "telegram")
            )
            assert not again.applied
            assert (
                await session.scalar(select(func.count()).select_from(TransactionalOutbox))
                == projections
            )
            card = await build_status_card(session, task_id)
            assert "start" not in card.buttons
            assert "Подготовка инструментов" in card.html

        class FixtureModel:
            async def execute(
                self, request: StepExecutionRequest, cancellation: CancellationContext
            ) -> StepOutcome:
                assert not cancellation.requested
                async with factory() as session:
                    wt = await session.scalar(
                        select(Worktree).where(Worktree.run_id == request.run_id)
                    )
                    assert wt is not None
                    if kind == "report":
                        return StepOutcome.succeeded(
                            {
                                "structured_output": {
                                    "summary": "Repository contains the initial README.",
                                    "findings": ["README starts with Before."],
                                    "sources": ["README.md:1"],
                                    "limitations": [],
                                }
                            }
                        )
                    Path(wt.path, "README.md").write_text("# After\n\nRequested fixture change.\n")
                return StepOutcome.succeeded({"implementation_summary": "Updated fixture docs."})

        class ReportReviewer:
            async def review(self, **kwargs: Any) -> ReviewVerdict:
                from vuzol.review.partitions import verify_chunk_receipts

                assert kind == "report"
                verify_chunk_receipts(kwargs["work_context"])
                assert kwargs["work_context"][0].source == "task_report"
                assert "README.md:1" in kwargs["work_context"][0].content
                assert kwargs["inspection"].changed_files == ()
                return ReviewVerdict(
                    verdict=ReviewVerdictKind.PASSED,
                    review_kind="independent",
                    risk="low",
                    base_commit=kwargs["base_commit"],
                    result_commit=kwargs["result_commit"],
                    diff_hash=kwargs["diff_hash"],
                    changed_files=(),
                    findings=(),
                    summary="Report matches the task and sources.",
                )

        artifacts = ArtifactStore(settings.artifact_root, max_bytes=1_000_000, retention_days=1)
        internal = WorkflowWorker(
            settings,
            factory,
            owner="common",
            handlers={
                **BASE_INTERNAL_HANDLERS,
                "review": ResultReviewHandler(
                    factory,
                    git,
                    worktree_root=settings.worktree_root,
                    independent_reviewer=ReportReviewer(),
                ),
            },
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
                "execute_code": FixtureModel(),
                "execute_agent": FixtureModel(),
                "ensure_dependencies": DependencyProvisioningHandler(
                    factory,
                    settings.dependency_provisioning,
                    None,
                    worktree_root=settings.worktree_root,
                ),
                "validate": ResultValidationHandler(
                    factory, runtime.registries, git, worktree_root=settings.worktree_root
                ),
                "produce_artifacts": ArtifactProductionHandler(
                    factory,
                    runtime.registries,
                    git,
                    cast(Any, MagicMock()),
                    cast(Any, MagicMock()),
                    artifacts,
                    worktree_root=settings.worktree_root,
                ),
                "build_static": StaticBuildHandler(
                    factory,
                    runtime.registries,
                    git,
                    cast(Any, MagicMock()),
                    cast(Any, MagicMock()),
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
        workers = (internal, heavy, applier, publisher)
        try:
            for _ in range(30):
                progressed = [await w.process_one() for w in workers]
                if not any(progressed):
                    break
            async with factory() as session:
                run = await session.get(Run, run_id)
                task = await session.get(Task, task_id)
                assert run is not None and run.status is RunStatus.RUNNING
                assert task is not None and task.status is TaskStatus.WAITING_APPROVAL
                steps = list(
                    await session.scalars(
                        select(Step).where(Step.run_id == run_id).order_by(Step.ordinal)
                    )
                )
                approval = await session.scalar(select(Approval))
                assert approval is not None and approval.status is ApprovalStatus.PENDING
                approval_id = approval.id
                boundary = next(s for s in steps if s.step_type == "approval")
                assert all(
                    s.status is StepStatus.COMPLETED for s in steps if s.ordinal < boundary.ordinal
                )
                assert boundary.status is StepStatus.WAITING_APPROVAL
                assert all(
                    s.status is StepStatus.PENDING for s in steps if s.ordinal > boundary.ordinal
                )
                if kind == "report":
                    assert boundary.payload["action_envelope"]["report_hash"]
                    from vuzol.workflows.result_approval import _validation_evidence

                    report_step = next(s for s in steps if s.step_type == "execute_agent")
                    original_result = report_step.result
                    report_step.result = {
                        "structured_output": {
                            **original_result["structured_output"],
                            "summary": "Changed after review",
                        }
                    }
                    tree = await session.scalar(select(Worktree).where(Worktree.run_id == run_id))
                    assert tree is not None
                    with pytest.raises(ValueError, match="report evidence changed"):
                        _validation_evidence({s.ordinal: s for s in steps}, tree)
                    report_step.result = original_result
                else:
                    env = await session.scalar(select(ProjectEnvironmentRevision))
                    assert (
                        env is not None
                        and env.source == "detected"
                        and env.contract["components"] == {}
                    )
                if kind != "coding.v3":
                    assert all(
                        s.step_type not in {"plan", "acceptance", "publish_preview"} for s in steps
                    )
                assert "start" not in (await build_status_card(session, task_id)).buttons
            assert await git.resolve_commit(repository, "main") == base
            assert not settings.static_site_root.exists()
            assert not settings.preview_site_root.exists()
            assert not await applier.process_one()
            async with factory.begin() as session:
                await decide_result(session, approval_id, decision="approve", deciding_user_id=1)
            for _ in range(10):
                if not any([await w.process_one() for w in workers]):
                    break
            async with factory() as session:
                run = await session.get(Run, run_id)
                task = await session.get(Task, task_id)
                approval = await session.get(Approval, approval_id)
                assert run is not None and run.status is RunStatus.COMPLETED
                assert task is not None and task.status is TaskStatus.COMPLETED
                assert approval is not None and approval.status is ApprovalStatus.CONSUMED
                assert all(
                    s is StepStatus.COMPLETED
                    for s in await session.scalars(select(Step.status).where(Step.run_id == run_id))
                )
                card = await build_status_card(session, task_id)
                assert f"Завершение ({step_status_label(StepStatus.COMPLETED)})" in card.html
                assert not card.buttons
            if kind == "report":
                assert await git.resolve_commit(repository, "main") == base
                assert (repository / "README.md").read_text() == "# Before\n"
            else:
                assert await git.resolve_commit(repository, "main") != base
                assert (repository / "README.md").read_text().startswith("# After")
            assert not settings.static_site_root.exists()
        finally:
            await registry.close()
            await engine.dispose()

    asyncio.run(scenario())
