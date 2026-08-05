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


def test_commit_message_uses_complete_title_and_local_task_number() -> None:
    title = "Добавить автоматическую категоризацию банковских транзакций"

    message = build_commit_message(
        CommitMessageContext(
            task_id=uuid.uuid4(),
            task_number=42,
            operation="create",
            normalized_title=f"  {title}\n",
        )
    )

    assert message == f"feat: {title}\n\nVuzol-Task: 42"


@pytest.mark.parametrize(
    ("operation", "prefix"),
    (("fix", "fix"), ("explain", "docs"), ("modify", "chore"), (None, "chore")),
)
def test_commit_type_follows_normalized_operation(operation: str | None, prefix: str) -> None:
    message = build_commit_message(
        CommitMessageContext(
            task_id=uuid.uuid4(),
            task_number=1,
            operation=operation,
            normalized_title="Complete title",
        )
    )
    assert message.startswith(f"{prefix}: Complete title")


def test_commit_message_uses_uuid_only_without_local_number() -> None:
    task_id = uuid.uuid4()
    message = build_commit_message(
        CommitMessageContext(
            task_id=task_id,
            task_number=None,
            operation="fix",
            normalized_title="Fix totals",
        )
    )
    assert message.endswith(f"Vuzol-Task: {task_id}")


@pytest.mark.anyio
async def test_database_resolver_loads_task_contract() -> None:
    task_id = uuid.uuid4()
    task = SimpleNamespace(
        project_id="demo",
        topic_task_number=7,
        task_draft={"operation": "fix", "normalized_title": "Fix checkout totals"},
        original_text="fallback",
    )
    session = AsyncMock()
    session.get.return_value = task
    factory = MagicMock(return_value=AsyncContext(session))

    message = await DatabaseCommitMessageResolver(factory).resolve(
        task_id=task_id, run_id=uuid.uuid4(), project_id="demo"
    )

    assert message == "fix: Fix checkout totals\n\nVuzol-Task: 7"


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
async def test_database_resolver_falls_back_to_summary() -> None:
    session = AsyncMock()
    session.get.return_value = SimpleNamespace(
        project_id="demo",
        topic_task_number=3,
        task_draft={"operation": "explain", "task_summary": "Document the API"},
        original_text="raw request",
    )
    factory = MagicMock(return_value=AsyncContext(session))

    message = await DatabaseCommitMessageResolver(factory).resolve(
        task_id=uuid.uuid4(), run_id=uuid.uuid4(), project_id="demo"
    )

    assert message.startswith("docs: Document the API")
