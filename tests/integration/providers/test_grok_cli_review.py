"""Grok review uses real fences/accounting, without a paid model invocation."""

import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select

from tests.integration.providers._test_routing_helpers import profile
from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.config import RuntimeConfiguration, Settings
from vuzol.execution.domain import GitInspection
from vuzol.providers.cli_readonly import ReadOnlyCliAdapter
from vuzol.providers.domain import ContextItem
from vuzol.providers.errors import ProviderFailure
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.ports import CodexProcessResult
from vuzol.review.independent import DatabaseReviewAccounting, IndependentModelReviewer
from vuzol.storage.errors import LeaseLost
from vuzol.storage.models import ProviderBudgetReservation, Step, Task, UsageRecord, Worktree
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import RiskLevel, StepStatus, WorktreeDeliveryState
from vuzol.workflows.ports import CancellationContext

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_cli_review_partitions_and_accounting(postgres_dsn, tmp_path):
    engine, factory = storage(postgres_dsn)
    try:
        configured = profile(
            "grok-a",
            provider="grok",
            model="grok-4.7",
            model_reasoning_effort="high",
            launch_mode="cli",
            api_base_url=None,
            roles={"reviewer"},
            runtime_identity="grok-a",
            state_directory=tmp_path / "state",
            context_limit=100000,
            output_limit=1000,
        )
        task_record, run_id, step_record = await seed_task_run_step(factory, step_type="review")
        expires = datetime.now(UTC) + timedelta(minutes=10)
        worktree_id = uuid.uuid4()
        async with factory.begin() as session:
            task = await session.get(Task, task_record.id)
            task.project_id = "p1"
            step = await session.get(Step, step_record.id)
            step.status = StepStatus.RUNNING
            step.lease_owner = "reviewer"
            step.lease_generation = 1
            step.lease_expires_at = expires
            session.add(
                Worktree(
                    id=worktree_id,
                    task_id=task.id,
                    run_id=run_id,
                    project_id="p1",
                    repository_identity_hash="c" * 64,
                    base_commit="a" * 40,
                    result_commit="b" * 40,
                    default_branch="main",
                    expected_target_head="a" * 40,
                    branch="review",
                    path=str(tmp_path / "worktree"),
                    owner="executor",
                    delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                    retention_until=expires,
                )
            )
        lease = LeaseToken(
            step=StepRecord(
                id=step_record.id,
                run_id=run_id,
                status=StepStatus.RUNNING,
                lease_generation=1,
                lease_owner="reviewer",
                lease_expires_at=expires,
            ),
            owner="reviewer",
            generation=1,
        )
        settings = Settings(environment="test", worktree_root=tmp_path)
        registries = MagicMock()
        registries.profiles.get.return_value = configured
        registries.sandboxes.get.return_value = SimpleNamespace(uid=10001, gid=10001)
        access = SimpleNamespace(revoke=AsyncMock())
        invocations = []

        class Transport:
            async def run(self, invocation, cancellation):
                invocations.append(invocation)
                assert invocation.sandbox_reference == f"worktree:{worktree_id}"
                assert invocation.argv[-2:] == ("--reasoning-effort", "high")
                assert "Edit(**)" not in invocation.argv
                assert "пока не запускай" in invocation.stdin
                return CodexProcessResult(
                    0,
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "text",
                                    "data": json.dumps(
                                        {"verdict": "pass", "summary": "reviewed", "findings": []}
                                    ),
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "end",
                                    "stopReason": "EndTurn",
                                    "usage": {"input_tokens": 30, "output_tokens": 10},
                                }
                            ),
                        ]
                    ),
                    "",
                    1,
                )

        wrapper = ReadOnlyCliAdapter(
            GrokCliAdapter(Transport()),
            RuntimeConfiguration.model_construct(settings=settings, registries=registries),
            factory,
        )
        wrapper._access = SimpleNamespace(grant=AsyncMock(return_value=access))
        adapters = MagicMock()
        adapters.get.return_value = wrapper
        reviewer = IndependentModelReviewer(
            registries,
            adapters,
            DatabaseReviewAccounting(factory, settings.limits),
            profile_id=configured.id,
        )
        text = "Полная спецификация; если условие ложно, ничего не меняй; пока не запускай."
        context = (
            ContextItem(
                source="chat_work_context",
                reference="task:spec:part-1-of-1",
                content=text,
                content_hash=hashlib.sha256(text.encode()).hexdigest(),
            ),
        )
        verdict = await reviewer.review(
            task=task,
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40,
                branch="review",
                changed_files=tuple(f"f{i}.py" for i in range(100)),
                diff=b"+safe\n",
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[],
            mechanical_findings=(),
            request_ids=(task.id, run_id, step_record.id),
            timeout_seconds=30,
            cancellation=CancellationContext(),
            lease=lease,
            work_context=context,
        )
        assert verdict.allows_progress and verdict.partition_count == 2
        assert len({i.provider_attempt for i in invocations}) == 3
        assert access.revoke.await_count == 3
        async with factory() as session:
            rows = (await session.scalars(select(UsageRecord))).all()
            reservations = (await session.scalars(select(ProviderBudgetReservation))).all()
            assert len(rows) == len(reservations) == 3
            assert {r.provider_attempt for r in reservations} == {
                i.provider_attempt for i in invocations
            }
            stored = await session.get(Step, step_record.id)
            assert stored.executor_profile_id == configured.id
            assert stored.payload["executor_reasoning_effort"] == "high"
        # A changed role assignment, stale lease, or cancellation must never
        # launch another process or silently fall back to the API reviewer.
        # Reuse a complete request built by the reviewer for guard checks.
        from vuzol.review.independent import _build_request

        request = _build_request(
            task=task,
            risk=RiskLevel.HIGH,
            inspection=GitInspection(
                head="b" * 40, branch="review", changed_files=("a",), diff=b"+a"
            ),
            base_commit="a" * 40,
            result_commit="b" * 40,
            diff_hash=None,
            gates=[],
            mechanical_findings=(),
            task_id=task.id,
            run_id=run_id,
            step_id=step_record.id,
            timeout_seconds=30,
            profile=configured,
            policy_revision="test",
            provider_attempt=4,
        )
        request = request.model_copy(update={"lease_generation": 1})
        with pytest.raises(LeaseLost, match="assignment changed"):
            await wrapper.execute(
                request, configured.model_copy(update={"model": "other"}), CancellationContext()
            )
        with pytest.raises(LeaseLost, match="stale"):
            await wrapper.execute(
                request.model_copy(update={"lease_generation": 2}),
                configured,
                CancellationContext(),
            )
        async with factory.begin() as session:
            # Existing activity by the same subscription consumes its one slot.
            other = Step(
                run_id=run_id,
                ordinal=2,
                step_type="execute_code",
                status=StepStatus.RUNNING,
                executor_profile_id=configured.id,
                lease_expires_at=expires,
                idempotency_class="idempotent",
                retry_class="never",
                timeout_seconds=30,
            )
            session.add(other)
        with pytest.raises(ProviderFailure, match="busy"):
            await wrapper.execute(request, configured, CancellationContext())
        assert len(invocations) == 3
        cancelled = CancellationContext()
        cancelled.request()
        with pytest.raises(LeaseLost, match="cancelled"):
            await wrapper.execute(request, configured, cancelled)
        async with factory.begin() as session:
            other = await session.get(Step, other.id)
            other.status = StepStatus.COMPLETED
        wrapper._adapter = SimpleNamespace(
            execute=AsyncMock(side_effect=RuntimeError("transport failed"))
        )
        with pytest.raises(RuntimeError, match="transport failed"):
            await wrapper.execute(request, configured, CancellationContext())
        assert access.revoke.await_count == 4
    finally:
        await engine.dispose()
