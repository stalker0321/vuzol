"""D6 composition unit tests: review policy, retrieval chain, compat, AST."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.domain import GitInspection
from vuzol.providers.planner_handoff import PLANNER_CONTEXT_SOURCE, load_planner_context_for_run
from vuzol.research.retrieval import (
    ApprovedHttpRetrieval,
    RetrievalBounds,
    TransportResponse,
)
from vuzol.research.source_backed import assemble_source_report
from vuzol.review.domain import ReviewVerdictKind
from vuzol.review.handler import ResultReviewHandler
from vuzol.storage.models import Step
from vuzol.storage.records import LeaseToken, StepRecord
from vuzol.storage.types import QueueClass, RiskLevel, StepStatus, WorktreeDeliveryState
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import verified_envelope


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> object:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _lease() -> LeaseToken:
    return LeaseToken(
        step=StepRecord(
            id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            status=StepStatus.RUNNING,
            lease_generation=1,
            lease_owner="owner",
            lease_expires_at=None,
        ),
        owner="owner",
        generation=1,
    )


def _request(task_id: uuid.UUID, run_id: uuid.UUID, lease: LeaseToken) -> StepExecutionRequest:
    return StepExecutionRequest(
        task_id=task_id,
        run_id=run_id,
        step_id=lease.step.id,
        step_type="review",
        payload={},
        timeout_seconds=120,
        lease=lease,
    )


@pytest.mark.anyio
async def test_medium_review_invokes_expected_policy_reviewer(tmp_path: Path) -> None:
    """Actual MEDIUM review invokes the expected policy reviewer (L2 independent)."""

    from vuzol.review.domain import ReviewVerdict

    base = "a" * 40
    result = "b" * 40
    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    validate = SimpleNamespace(
        step_type="validate",
        status=StepStatus.COMPLETED,
        result={
            "structured_output": {
                "base_commit": base,
                "result_commit": result,
                "gates": [{"exit_code": 0}],
            }
        },
    )
    session = MagicMock()
    session.get = AsyncMock(
        side_effect=[
            SimpleNamespace(
                status=StepStatus.RUNNING,
                lease_owner=lease.owner,
                lease_generation=lease.generation,
                run_id=run_id,
                payload={},
                dependency_metadata={"predecessor_ordinals": [5]},
            ),
            SimpleNamespace(task_id=task_id),
            SimpleNamespace(risk=RiskLevel.MEDIUM, task_draft={}),
        ]
    )
    session.scalar = AsyncMock(
        side_effect=[
            validate,
            SimpleNamespace(
                path=str(tmp_path / "wt"),
                delivery_state=WorktreeDeliveryState.WORKTREE_RETAINED,
                base_commit=base,
                result_commit=result,
                diff_hash="c" * 64,
                branch="task-branch",
            ),
        ]
    )
    factory = MagicMock(return_value=AsyncContext(session))
    git = MagicMock()
    git.require_clean_worktree = AsyncMock()
    git.require_no_remotes = AsyncMock()
    git.inspect = AsyncMock(
        return_value=GitInspection(
            head=result,
            branch="task-branch",
            changed_files=("src/service.py",),
            diff=b"+code\n",
        )
    )
    independent = MagicMock()
    independent.review = AsyncMock(
        return_value=ReviewVerdict(
            verdict=ReviewVerdictKind.PASSED,
            review_kind="independent",
            risk="medium",
            base_commit=base,
            result_commit=result,
            diff_hash="c" * 64,
            changed_files=("src/service.py",),
            findings=(),
            summary="Independent review passed.",
        )
    )
    worktree_path = tmp_path / "wt"
    worktree_path.mkdir()
    handler = ResultReviewHandler(
        factory, git, worktree_root=tmp_path, independent_reviewer=independent
    )
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["verdict"] == ReviewVerdictKind.PASSED.value
    assert outcome.result["review_kind"] == "independent"
    independent.review.assert_awaited_once()


@pytest.mark.anyio
async def test_stale_review_lease_is_rejected(tmp_path: Path) -> None:
    """A review bound to a superseded lease never executes (drill: stale review)."""

    lease = _lease()
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    session = MagicMock()
    session.get = AsyncMock(
        return_value=SimpleNamespace(
            status=StepStatus.RUNNING,
            lease_owner="other-worker",
            lease_generation=lease.generation + 1,
            run_id=run_id,
            payload={},
        )
    )
    factory = MagicMock(return_value=AsyncContext(session))
    handler = ResultReviewHandler(factory, MagicMock(), worktree_root=tmp_path)
    # execute() converts the fence breach into BLOCKED, never a runaway review.
    outcome = await handler.execute(_request(task_id, run_id, lease), CancellationContext())
    assert outcome.kind is OutcomeKind.BLOCKED
    assert outcome.category == "review_failed"


class _StubTransport:
    def __init__(self, responses: dict[str, TransportResponse]) -> None:
        self.responses = responses

    def get(self, uri: str, bounds: RetrievalBounds) -> TransportResponse:
        return self.responses[uri]


@pytest.mark.anyio
async def test_http_adapter_to_report_to_planner_context() -> None:
    """HTTP adapter → source report → planner context, hash-chained."""

    body = b"Postgres 16 supports logical replication. " + b"x" * 160
    adapter = ApprovedHttpRetrieval(
        allowlist=frozenset({"example.com"}),
        transport=_StubTransport({"https://example.com/repl": TransportResponse(200, body)}),
    )
    fetched = adapter.fetch("https://example.com/repl", now="2026-09-30T10:00:00Z")
    assert fetched.content_hash == hashlib.sha256(body).hexdigest()

    def fetch(uri: str, *, now: str) -> object:
        assert uri == "https://example.com/repl"
        assert now == "2026-09-30T10:00:00Z"
        return fetched

    report_bytes, _blobs, anchor = assemble_source_report(
        structured_output={
            "research": {
                "question": "Which Postgres version supports logical replication?",
                "sources": [{"uri": "https://example.com/repl"}],
                "claims": [
                    {
                        "claim_id": "c1",
                        "statement": "Postgres 16 supports logical replication",
                        "support": "supported",
                        "citations": [["https://example.com/repl", "offset:0-40"]],
                    }
                ],
            }
        },
        task_question="Which Postgres version supports logical replication?",
        scope="project",
        created_at="2026-09-30T10:00:00Z",
        fetch=fetch,  # type: ignore[arg-type]
        retriever="approved-http",
    )
    assert anchor == "2026-09-30T10:00:00Z"
    assert b"logical replication" in report_bytes

    step = Step(
        id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        ordinal=1,
        dependency_metadata={},
        step_type="plan",
        queue_class=QueueClass.LIGHT,
        status=StepStatus.COMPLETED,
        required_capabilities=[],
        payload={},
        result={
            "text": "Plan: use Postgres 16 logical replication. Then validate.",
            "finish_reason": "stop",
            "handoff": {"status": "ready"},
        },
    )
    items = load_planner_context_for_run(step)
    assert len(items) == 1
    assert items[0].source == PLANNER_CONTEXT_SOURCE
    assert "logical replication" in items[0].content
    assert items[0].content_hash == hashlib.sha256(items[0].content.encode()).hexdigest()


def test_ast_no_call_diagnostic_with_pinned_allowlist() -> None:
    """AST diagnostic: no new subprocess/kill call sites outside the pin."""

    import ast

    root = Path(__file__).resolve().parents[2] / "src" / "vuzol"
    # Pinned process-supervision surface (verified 2026-09-30): timeout and
    # cleanup kills plus finite-argv runs. Any new call site fails this test
    # and must be justified as supervised (timeout + bounded argv).
    pinned = {
        "cli/agent_certify.py",
        "execution/access.py",
        "execution/git.py",
        "execution/proxy_networks.py",
        "execution/proxy_service.py",
        "execution/sandbox.py",
        "experiments/review.py",
        "ops/backup/postgres_dump.py",
        "ops/backup/postgres_restore.py",
        "ops/production_deploy.py",
        "projects/provisioning.py",
        "projects/runner.py",
        "scout.py",
        "workflows/runtime_preview.py",
    }
    offenders: list[str] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_bytes(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "subprocess":
                offenders.append(str(path.relative_to(root)))
                break
            if isinstance(node, ast.Attribute) and node.attr in {"kill", "killpg"}:
                offenders.append(str(path.relative_to(root)))
                break
            if isinstance(node, ast.Attribute) and node.attr in {"run", "Popen"}:
                value = node.value
                if (isinstance(value, ast.Name) and value.id == "subprocess") or (
                    isinstance(value, ast.Attribute) and value.attr == "subprocess"
                ):
                    offenders.append(str(path.relative_to(root)))
                    break
    assert sorted(set(offenders)) == sorted(pinned)


def test_compatibility_corpus_reads_legacy_shapes() -> None:
    """Old drafts, workflows and approvals validate against current code."""

    from vuzol.interpretation.domain import (
        SuggestedComplexity,
        TaskAction,
        TaskDraft,
        TaskOperation,
        TaskType,
    )
    from vuzol.storage.types import RiskLevel
    from vuzol.workflows.compiler import compile_workflow

    # Pre-1.4 draft without task_summary derives it (append-only compat).
    legacy = TaskDraft.model_validate(
        {
            "action": "create_task",
            "task_type": "coding",
            "operation": "modify",
            "goal": "Implement the request",
            "suggested_complexity": "small",
            "suggested_risk": "low",
            "needs_clarification": False,
            "normalized_title": "Implement request",
        }
    )
    assert legacy.task_summary == "Implement request"
    workflow = compile_workflow(legacy, interpretation_id=uuid.uuid4())
    assert workflow.workflow_type == "coding"

    modern = TaskDraft(
        action=TaskAction.CREATE_TASK,
        task_type=TaskType.CODING,
        operation=TaskOperation.MODIFY,
        goal="Implement the request",
        task_summary="Implement the request",
        suggested_complexity=SuggestedComplexity.SMALL,
        suggested_risk=RiskLevel.LOW,
        needs_clarification=False,
        normalized_title="Implement request",
    )
    assert compile_workflow(modern, interpretation_id=uuid.uuid4()).version in {"1", "4"}

    # Legacy approval envelope without D2 revision fields passes through.
    import hashlib as _hashlib
    import json as _json

    step_id = uuid.uuid4()
    envelope = {"step_id": str(step_id), "action": "apply_result"}
    digest = _hashlib.sha256(
        _json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    legacy_step = SimpleNamespace(id=step_id, payload={"action_envelope": dict(envelope)})
    legacy_approval = SimpleNamespace(action_envelope_hash=digest)
    assert verified_envelope(legacy_step, legacy_approval) == envelope  # type: ignore[arg-type]
