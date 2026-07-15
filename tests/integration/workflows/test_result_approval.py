import asyncio
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from vuzol.config import DeliveryMode, GitDeliveryPolicy
from vuzol.execution.git import LocalGit
from vuzol.execution.result_apply import ResultApplyHandler
from vuzol.storage.models import Approval, Run, Step, Task, Worktree
from vuzol.storage.types import (
    ApprovalStatus,
    IdempotencyClass,
    QueueClass,
    RetryClass,
    RiskLevel,
    RunStatus,
    StepStatus,
    TaskStatus,
    WorktreeDeliveryState,
)
from vuzol.telegram.projections import build_status_card
from vuzol.workflows.controls import decide_result
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext
from vuzol.workflows.result_approval import ensure_result_approval, envelope_hash

from ..storage.helpers import storage

pytestmark = pytest.mark.postgresql


def test_retained_result_projection_and_approval_are_bound_to_one_envelope(
    postgres_dsn: str, tmp_path: Path
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(("git", "init", "-b", "main", str(repository)), check=True)
    subprocess.run(("git", "-C", str(repository), "config", "user.name", "Test"), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "config", "user.email", "test@example.invalid"),
        check=True,
    )
    (repository / "value.txt").write_text("base\n")
    subprocess.run(("git", "-C", str(repository), "add", "."), check=True)
    subprocess.run(
        ("git", "-C", str(repository), "commit", "-m", "base"),
        check=True,
        capture_output=True,
    )
    base = subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ("git", "-C", str(repository), "switch", "--detach"),
        check=True,
        capture_output=True,
    )

    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task_id = uuid.uuid4()
        run_id = uuid.uuid4()
        source_step_id = uuid.uuid4()
        approval_step_id = uuid.uuid4()
        git = LocalGit()
        worktree_path = tmp_path / "result-worktree"
        await git.add_worktree(repository, worktree_path, "step09a/test/result", base)
        (worktree_path / "value.txt").write_text("approved\n")
        await git.stage_paths(worktree_path, ("value.txt",))
        result_commit = await git.create_commit(worktree_path, "approved result")
        inspection = await git.inspect(worktree_path, base)
        repository_identity, _remote = await git.repository_identity(repository)
        async with factory.begin() as session:
            task = Task(
                id=task_id,
                user_id=42,
                source_chat_id=-100,
                source_thread_id=None,
                project_id="vuzol",
                original_text="bounded task",
                task_draft={"normalized_title": "Bounded task"},
                status=TaskStatus.WAITING_APPROVAL,
                risk=RiskLevel.LOW,
                task_type="coding",
            )
            run = Run(
                id=run_id,
                task_id=task_id,
                workflow_type="adaptive_worker_trial",
                workflow_version="1",
                status=RunStatus.RUNNING,
                selected_route={"trusted_profile_id": "sol-subscription-a"},
                budget_mode="strong",
                configuration_revision="c" * 64,
                policy_revision="d" * 64,
            )
            source_step = Step(
                id=source_step_id,
                run_id=run_id,
                ordinal=2,
                dependency_metadata={"predecessor_ordinals": [1]},
                step_type="execute_code",
                queue_class=QueueClass.HEAVY,
                status=StepStatus.COMPLETED,
                required_capabilities=[],
                payload={},
                result={
                    "implementation_summary": "Added the requested validator <safely>.",
                    "structured_output": {
                        "base_commit": base,
                        "result_commit": result_commit,
                        "gates": [
                            {
                                "name": "tests",
                                "command_id": "make test",
                                "exit_code": 0,
                                "duration_ms": 1250,
                            }
                        ],
                    },
                },
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.UNKNOWN_EFFECTS_POSSIBLE,
                max_attempts=1,
                timeout_seconds=60,
            )
            approval_step = Step(
                id=approval_step_id,
                run_id=run_id,
                ordinal=3,
                dependency_metadata={"predecessor_ordinals": [2]},
                step_type="approval",
                queue_class=QueueClass.PRIVILEGED,
                status=StepStatus.WAITING_APPROVAL,
                required_capabilities=["git"],
                payload={"requested_action": "apply_result"},
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                max_attempts=1,
                timeout_seconds=120,
            )
            worktree = Worktree(
                task_id=task_id,
                run_id=run_id,
                project_id="vuzol",
                repository_identity_hash=repository_identity,
                base_commit=base,
                default_branch="main",
                expected_target_head=base,
                branch="step09a/test/result",
                path=str(worktree_path),
                owner="test",
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                result_commit=result_commit,
                diff_hash=inspection.diff_hash,
                retention_until=datetime.now(UTC) + timedelta(days=1),
            )
            session.add(task)
            await session.flush()
            session.add(run)
            await session.flush()
            session.add_all((source_step, approval_step, worktree))
            await session.flush()
            approval = await ensure_result_approval(
                session,
                run=run,
                approval_step=approval_step,
                steps_by_ordinal={2: source_step, 3: approval_step},
            )
            assert approval is not None
            assert approval_step.external_idempotency_key == (
                f"apply-result:{approval.action_envelope_hash}"
            )

        async with factory() as session:
            card = await build_status_card(session, task_id)
            assert "Added the requested validator &lt;safely&gt;." in card.html
            assert "tests — passed (1.2s)" in card.html
            assert result_commit not in card.html
            assert "diff" not in card.html.lower()
            assert card.buttons == ("approve", "redo", "reject")
            assert card.approval_id is not None

        async with factory.begin() as session:
            assert card.approval_id is not None
            await decide_result(
                session,
                card.approval_id,
                decision="approve",
                deciding_user_id=42,
            )

        async with factory() as session:
            approval = await session.scalar(select(Approval).where(Approval.id == card.approval_id))
            persisted_step = await session.get(Step, approval_step_id)
            persisted_task = await session.get(Task, task_id)
            assert approval is not None and approval.status is ApprovalStatus.APPROVED
            assert persisted_step is not None and persisted_step.status is StepStatus.QUEUED
            assert persisted_task is not None and persisted_task.status is TaskStatus.EXECUTING

        project = SimpleNamespace(
            repository_path=repository,
            git_delivery=GitDeliveryPolicy(
                allowed_modes=frozenset(
                    {DeliveryMode.RETAIN, DeliveryMode.PATCH, DeliveryMode.APPLY}
                ),
                approval_required=frozenset({DeliveryMode.APPLY}),
            ),
        )
        registries = MagicMock(revision="c" * 64)
        registries.projects.get.return_value = project
        request = MagicMock(
            task_id=task_id,
            run_id=run_id,
            step_id=approval_step_id,
            step_type="approval",
        )
        outcome = await ResultApplyHandler(factory, registries, git).execute(
            request, CancellationContext()
        )
        assert outcome.kind is OutcomeKind.SUCCEEDED
        assert await git.resolve_commit(repository, "main") == result_commit
        replay = await ResultApplyHandler(factory, registries, git).execute(
            request, CancellationContext()
        )
        assert replay.kind is OutcomeKind.SUCCEEDED
        async with factory() as session:
            approval = await session.scalar(select(Approval).where(Approval.id == card.approval_id))
            persisted_worktree = await session.scalar(
                select(Worktree).where(Worktree.run_id == run_id)
            )
            assert approval is not None and approval.status is ApprovalStatus.CONSUMED
            assert persisted_worktree is not None
            assert persisted_worktree.delivery_state is WorktreeDeliveryState.APPLIED
            assert persisted_worktree.delivered_ref == "refs/heads/main"
        await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("decision", "step_status", "run_status", "task_status"),
    (
        ("redo", StepStatus.CANCELLED, RunStatus.CANCELLED, TaskStatus.CANCELLED),
        ("reject", StepStatus.CANCELLED, RunStatus.CANCELLED, TaskStatus.CANCELLED),
    ),
)
def test_redo_and_reject_do_not_apply_the_retained_result(
    postgres_dsn: str,
    decision: str,
    step_status: StepStatus,
    run_status: RunStatus,
    task_status: TaskStatus,
) -> None:
    async def scenario() -> None:
        engine, factory = storage(postgres_dsn)
        task = Task(
            user_id=42,
            source_chat_id=-100,
            project_id="vuzol",
            original_text="task",
            task_draft={},
            status=TaskStatus.WAITING_APPROVAL,
            risk=RiskLevel.LOW,
            task_type="coding",
        )
        async with factory.begin() as session:
            session.add(task)
            await session.flush()
            run = Run(
                task_id=task.id,
                workflow_type="adaptive_worker_trial",
                workflow_version="1",
                status=RunStatus.RUNNING,
                selected_route={},
                budget_mode="strong",
                configuration_revision="a" * 64,
                policy_revision="b" * 64,
            )
            session.add(run)
            await session.flush()
            step = Step(
                run_id=run.id,
                ordinal=1,
                dependency_metadata={"predecessor_ordinals": [0]},
                step_type="approval",
                queue_class=QueueClass.PRIVILEGED,
                status=StepStatus.WAITING_APPROVAL,
                required_capabilities=["git"],
                payload={},
                retry_class=RetryClass.NEVER,
                idempotency_class=IdempotencyClass.IDEMPOTENT,
                max_attempts=1,
                timeout_seconds=120,
            )
            session.add(step)
            await session.flush()
            envelope = {"schema_version": "result-approval.v1", "step_id": str(step.id)}
            step.payload = {"action_envelope": envelope}
            approval = Approval(
                step_id=step.id,
                action_envelope_hash=envelope_hash(envelope),
                requested_action="apply_result",
                normalized_target="vuzol:main",
                human_summary="done",
                token_hash=uuid.uuid4().hex + uuid.uuid4().hex,
                status=ApprovalStatus.PENDING,
                expires_at=datetime.now(UTC) + timedelta(days=1),
            )
            session.add(approval)
            await session.flush()
            approval_id = approval.id
            step_id = step.id
            run_id = run.id
            task_id = task.id

        async with factory.begin() as session:
            await decide_result(
                session,
                approval_id,
                decision=decision,
                deciding_user_id=42,
            )
        async with factory() as session:
            persisted_approval = await session.get(Approval, approval_id)
            persisted_step = await session.get(Step, step_id)
            persisted_run = await session.get(Run, run_id)
            persisted_task = await session.get(Task, task_id)
            assert persisted_approval is not None
            assert persisted_approval.status is ApprovalStatus.REJECTED
            assert persisted_step is not None and persisted_step.status is step_status
            assert persisted_run is not None and persisted_run.status is run_status
            assert persisted_task is not None and persisted_task.status is task_status
        await engine.dispose()

    asyncio.run(scenario())
