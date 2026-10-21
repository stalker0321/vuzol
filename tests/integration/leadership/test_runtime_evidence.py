"""Trusted validator persistence -> applier snapshot -> independent review transport."""

import hashlib
import json

import pytest
from sqlalchemy import select
from tests.integration.leadership import test_managed_goal_flow as flow

from vuzol.execution.finalization import GateEvidence
from vuzol.execution.result_validation import ResultValidationHandler, _success_payload
from vuzol.leadership import verification
from vuzol.leadership.evidence import runtime_validation
from vuzol.leadership.persistence import ManagedGoal
from vuzol.storage.models import Step, ValidationResult, WorkPackage, Worktree
from vuzol.workflows.domain import StepOutcome

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


async def test_validation_receipts_reach_review_and_are_bound_to_the_applied_commit(
    postgres_dsn, tmp_path, monkeypatch
):
    original_workers = flow._workflow_workers
    original_invoke = verification.invoke
    seen = []

    def workers(runtime, factory, git, worktrees):
        result = original_workers(runtime, factory, git, worktrees)
        heavy = result[1]
        validator = ResultValidationHandler(
            factory, runtime.registries, git, worktree_root=runtime.settings.worktree_root
        )

        class Validated:
            async def execute(self, request, cancellation):
                async with factory() as session:
                    row = await session.scalar(
                        select(Worktree).where(Worktree.run_id == request.run_id)
                    )
                    worktree_id = row.id
                    # Fake only the external gate observation. Persistence, fences,
                    # queue outcome, integration apply and reviewer composition are real.
                    evidence = _success_payload(
                        base_commit=row.base_commit,
                        result_commit=row.result_commit,
                        branch=row.branch,
                        changed_files=("MARKER.txt",),
                        diff_hash=row.diff_hash,
                        system_checks=[],
                        gates=(
                            GateEvidence(
                                name="tests",
                                command_id="make test",
                                argv=("/usr/bin/make", "test"),
                                exit_code=0,
                                duration_ms=3,
                                stdout_sha256="a" * 64,
                                stdout_bytes=15,
                                stdout_truncated=False,
                                stderr_sha256="b" * 64,
                                stderr_bytes=0,
                                stderr_truncated=False,
                                validation_image_digest="fixture@sha256:" + "c" * 64,
                            ),
                        ),
                    )
                await validator._persist(request, worktree_id=worktree_id, evidence=evidence)
                return StepOutcome.succeeded(evidence)

        heavy._handlers["validate"] = Validated()
        return result

    async def invoke(*args, **kwargs):
        body = json.loads(args[3].original_input.split("\n", 1)[1])
        receipts = body["runtime_validation"]
        assert receipts, "The independent reviewer never received runtime test evidence"
        for receipt in receipts:
            assert receipt["result_commit"] == body["result_commit"]
            assert receipt["scope_revision"] == body["scope_revision"]
            assert receipt["gate"]["argv"] == ["/usr/bin/make", "test"]
            assert receipt["gate"]["stdout_sha256"] == "a" * 64
            assert receipt["gate"]["exit_code"] == 0
            assert receipt["command_hash"] == hashlib.sha256(b"/usr/bin/make\0test").hexdigest()
        seen.append(body)
        returned = await original_invoke(*args, **kwargs)
        payload = returned.structured_output
        for verdict in payload["criteria"]:
            if verdict["verdict"] == "pass":
                verdict["evidence_paths"].append(receipts[0]["evidence_path"])
        return returned.model_copy(update={"structured_output": payload})

    monkeypatch.setattr(flow, "_workflow_workers", workers)
    monkeypatch.setattr(verification, "invoke", invoke)
    await flow.run_managed_goal_flow(postgres_dsn, tmp_path, monkeypatch)
    assert len(seen) >= 2
    assert len({body["result_commit"] for body in seen}) >= 2
    async with flow._factory(postgres_dsn) as factory, factory.begin() as session:
        goal = await session.scalar(select(ManagedGoal))
        package = await session.get(WorkPackage, goal.current_package_id)
        kwargs = dict(
            package=package,
            result_commit=package.integration_head_commit,
            scope_revision=goal.scope_revision,
        )
        assert await runtime_validation(session, **kwargs)
        row = await session.scalar(
            select(ValidationResult)
            .join(Step)
            .join(Worktree, Worktree.run_id == Step.run_id)
            .where(Worktree.result_commit == package.integration_head_commit)
        )
        original_hash = row.command_hash
        row.command_hash = "0" * 64
        await session.flush()
        assert not await runtime_validation(session, **kwargs)
        row.command_hash = original_hash
        step = await session.get(Step, row.step_id)
        original = step.result
        for invalid in (
            {**original, "result_commit": "0" * 40},
            {**original, "diff_hash": "0" * 64},
            {**original, "schema_version": "executor-self-report"},
        ):
            step.result = invalid
            await session.flush()
            assert not await runtime_validation(session, **kwargs)
        step.result = original
        step.step_type = "execute_code"
        await session.flush()
        assert not await runtime_validation(session, **kwargs)
