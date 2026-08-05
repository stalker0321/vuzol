import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.commit_messages import (
    CommitMessageContext,
    DatabaseCommitMessageResolver,
    build_commit_message,
)


class AsyncContext:
    def __init__(self, value: object) -> None:
        self._value = value

    async def __aenter__(self) -> object:
        return self._value

    async def __aexit__(self, *_args: object) -> None:
        return None


def test_commit_message_uses_normalized_title_and_plan_provenance() -> None:
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    package_id = uuid.uuid4()
    revision_id = uuid.uuid4()

    message = build_commit_message(
        CommitMessageContext(
            task_id=task_id,
            run_id=run_id,
            project_id="bill_buddy",
            normalized_title="  Add transaction\n categorization.  ",
            work_package_id=package_id,
            plan_revision_id=revision_id,
            plan_ordinal=2,
            plan_item_count=4,
        )
    )

    lines = message.splitlines()
    assert lines[0] == "task(bill-buddy): Add transaction categorization"
    assert len(lines[0]) <= 72
    assert f"Vuzol-Task: {task_id}" in lines
    assert f"Vuzol-Run: {run_id}" in lines
    assert "Vuzol-Plan-Item: 2/4" in lines


def test_commit_message_bounds_long_title_and_omits_partial_plan_context() -> None:
    message = build_commit_message(
        CommitMessageContext(
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            project_id="Demo Project!",
            normalized_title="x" * 200,
            plan_ordinal=1,
        )
    )

    assert len(message.splitlines()[0]) == 72
    assert message.startswith("task(demo-project): ")
    assert "Vuzol-Plan-Item" not in message


@pytest.mark.anyio
async def test_database_resolver_loads_task_and_chain_position() -> None:
    task_id = uuid.uuid4()
    run_id = uuid.uuid4()
    package_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    task = SimpleNamespace(
        project_id="demo",
        task_draft={"normalized_title": "Fix checkout totals"},
        original_text="fallback",
    )
    link = SimpleNamespace(
        work_package_id=package_id,
        plan_revision_id=revision_id,
        ordinal=3,
    )
    session = AsyncMock()
    session.get.return_value = task
    session.scalar.side_effect = [link, 5]
    factory = MagicMock(return_value=AsyncContext(session))

    message = await DatabaseCommitMessageResolver(factory).resolve(
        task_id=task_id, run_id=run_id, project_id="demo"
    )

    assert message.startswith("task(demo): Fix checkout totals")
    assert "Vuzol-Plan-Item: 3/5" in message


@pytest.mark.anyio
async def test_database_resolver_fails_closed_on_wrong_project() -> None:
    session = AsyncMock()
    session.get.return_value = SimpleNamespace(project_id="other")
    factory = MagicMock(return_value=AsyncContext(session))

    with pytest.raises(LookupError, match="task context"):
        await DatabaseCommitMessageResolver(factory).resolve(
            task_id=uuid.uuid4(), run_id=uuid.uuid4(), project_id="demo"
        )


@pytest.mark.anyio
async def test_database_resolver_falls_back_to_summary_without_plan() -> None:
    session = AsyncMock()
    session.get.return_value = SimpleNamespace(
        project_id="demo",
        task_draft={"task_summary": "Document the API"},
        original_text="raw request",
    )
    session.scalar.return_value = None
    factory = MagicMock(return_value=AsyncContext(session))

    message = await DatabaseCommitMessageResolver(factory).resolve(
        task_id=uuid.uuid4(), run_id=uuid.uuid4(), project_id="demo"
    )

    assert message.startswith("task(demo): Document the API")
    assert "Vuzol-Plan-Item" not in message


def test_commit_message_has_safe_fallbacks() -> None:
    message = build_commit_message(
        CommitMessageContext(
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            project_id="!!!",
            normalized_title=" \n ",
        )
    )

    assert message.startswith("task(project): apply requested changes")
