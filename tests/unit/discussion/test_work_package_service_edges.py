import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.discussion.domain import DomainError
from vuzol.discussion.service import WorkPackageService
from vuzol.storage.models import WorkPackage
from vuzol.storage.types import (
    PlanRevisionState,
    WorkPackagePauseReason,
    WorkPackageStatus,
)


def _service() -> tuple[WorkPackageService, MagicMock]:
    uow = MagicMock()
    uow.session = MagicMock()
    uow.session.scalar = AsyncMock()
    uow.work_packages.get_package = AsyncMock()
    uow.work_packages.get_fenced_revision = AsyncMock()
    uow.work_packages.resolve_fenced_item = AsyncMock()
    uow.work_packages.close_open_edit_sessions = AsyncMock(return_value=[])
    return WorkPackageService(cast(Any, uow)), uow


def _args(package_id: uuid.UUID) -> dict[str, object]:
    return {
        "package_id": package_id,
        "revision_number": 1,
        "h8": "a" * 8,
        "expected_status_generation": 1,
    }


@pytest.mark.anyio
async def test_start_validation_rejects_status_and_approval_binding() -> None:
    service, uow = _service()
    package_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    package = SimpleNamespace(
        version=1,
        status=WorkPackageStatus.DRAFT,
        approved_revision_id=None,
    )
    uow.work_packages.get_package.return_value = package

    with pytest.raises(DomainError, match="invalid_transition"):
        await service.validate_startable(**_args(package_id))  # type: ignore[arg-type]

    package.status = WorkPackageStatus.APPROVED
    service._fenced_revision = AsyncMock(  # type: ignore[method-assign]
        return_value=SimpleNamespace(id=revision_id, state=PlanRevisionState.DRAFT)
    )
    with pytest.raises(DomainError, match="approval_binding_mismatch"):
        await service.validate_startable(**_args(package_id))  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_control_fence_returns_current_revision_and_rejects_stale() -> None:
    service, uow = _service()
    package_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    package = SimpleNamespace(
        version=1,
        status=WorkPackageStatus.DRAFT,
        head_revision_id=revision_id,
    )
    revision = SimpleNamespace(id=revision_id)
    uow.work_packages.get_package.return_value = package
    service._fenced_revision = AsyncMock(return_value=revision)  # type: ignore[method-assign]

    assert await service.validate_control_fence(**_args(package_id)) == (revision_id, 1)  # type: ignore[arg-type]
    package.head_revision_id = uuid.uuid4()
    with pytest.raises(DomainError, match="stale_revision"):
        await service.validate_control_fence(**_args(package_id))  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_pause_for_item_outcome_rejects_status_revision_and_cursor() -> None:
    service, uow = _service()
    package_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    package = SimpleNamespace(
        version=1,
        status=WorkPackageStatus.DRAFT,
        running_revision_id=revision_id,
        head_revision_id=revision_id,
        cursor_ordinal=1,
    )
    uow.work_packages.get_package.return_value = package
    service._fenced_revision = AsyncMock(  # type: ignore[method-assign]
        return_value=SimpleNamespace(id=revision_id)
    )
    service._resolve_item = AsyncMock(return_value=(revision_id, uuid.uuid4()))  # type: ignore[method-assign]
    arguments = {**_args(package_id), "ordinal": 1, "blocked": True}

    with pytest.raises(DomainError, match="invalid_transition"):
        await service.pause_for_item_outcome(**arguments)  # type: ignore[arg-type]

    package.status = WorkPackageStatus.RUNNING
    package.head_revision_id = uuid.uuid4()
    with pytest.raises(DomainError, match="stale_revision"):
        await service.pause_for_item_outcome(**arguments)  # type: ignore[arg-type]

    package.head_revision_id = revision_id
    service._resolve_item = AsyncMock(return_value=(uuid.uuid4(), uuid.uuid4()))  # type: ignore[method-assign]
    with pytest.raises(DomainError, match="stale_cursor"):
        await service.pause_for_item_outcome(**arguments)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_retry_and_skip_reject_incomplete_failure_contexts() -> None:
    service, _uow = _service()
    package_id = uuid.uuid4()
    revision = SimpleNamespace(id=uuid.uuid4())
    package = SimpleNamespace(
        id=package_id,
        version=1,
        status=WorkPackageStatus.PAUSED,
        head_revision_id=revision.id,
        pause_reason=WorkPackagePauseReason.REPLAN_REQUIRED,
        cursor_ordinal=1,
        last_failure_task_id=uuid.uuid4(),
    )
    service._queue_control_package = AsyncMock(return_value=(package, revision))  # type: ignore[method-assign]
    arguments = {**_args(package_id), "user_id": 42}

    with pytest.raises(DomainError, match="failure_context_missing"):
        await service.retry_item(**arguments)  # type: ignore[arg-type]

    package.pause_reason = WorkPackagePauseReason.ITEM_BLOCKED
    package.cursor_ordinal = None
    with pytest.raises(DomainError, match="failure_context_missing"):
        await service.retry_item(**arguments)  # type: ignore[arg-type]
    with pytest.raises(DomainError, match="failure_context_missing"):
        await service.skip_item(**arguments)  # type: ignore[arg-type]

    package.pause_reason = WorkPackagePauseReason.ITEM_FAILED
    package.cursor_ordinal = 1
    with pytest.raises(DomainError, match="item_not_safely_retryable"):
        await service.retry_item(**arguments)  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_retry_rejects_missing_blocked_step_and_queue_rejects_stale() -> None:
    service, uow = _service()
    package_id = uuid.uuid4()
    revision = SimpleNamespace(id=uuid.uuid4())
    package = SimpleNamespace(
        id=package_id,
        version=1,
        status=WorkPackageStatus.PAUSED,
        head_revision_id=revision.id,
        pause_reason=WorkPackagePauseReason.ITEM_BLOCKED,
        cursor_ordinal=1,
        last_failure_task_id=uuid.uuid4(),
    )
    service._queue_control_package = AsyncMock(return_value=(package, revision))  # type: ignore[method-assign]
    uow.session.scalar.return_value = None

    with pytest.raises(DomainError, match="item_not_safely_retryable"):
        await service.retry_item(**_args(package_id), user_id=42)  # type: ignore[arg-type]

    service._queue_control_package = WorkPackageService._queue_control_package.__get__(  # type: ignore[method-assign]
        service, WorkPackageService
    )
    uow.work_packages.get_package.return_value = package
    service._fenced_revision = AsyncMock(return_value=revision)  # type: ignore[method-assign]
    package.head_revision_id = uuid.uuid4()
    with pytest.raises(DomainError, match="stale_revision"):
        await service._queue_control_package(package_id, 1, "a" * 8, 1)


@pytest.mark.anyio
async def test_resolution_helpers_fail_closed_and_emit_close_events() -> None:
    service, uow = _service()
    package_id = uuid.uuid4()
    uow.work_packages.resolve_fenced_item.return_value = None

    with pytest.raises(DomainError, match="stale_revision"):
        await service._resolve_item(package_id, 1, "a" * 8, 1)

    uow.work_packages.get_fenced_revision.side_effect = LookupError
    with pytest.raises(DomainError, match="stale_revision"):
        await service._fenced_revision(package_id, 1, "a" * 8)

    edit_id = uuid.uuid4()
    uow.work_packages.close_open_edit_sessions.return_value = [edit_id]
    service._event = AsyncMock(return_value=uuid.uuid4())  # type: ignore[method-assign]
    await service._close_open_edits(package_id, actor_type="system")
    service._event.assert_awaited_once()


def test_failure_pause_guard_rejects_non_failure_reason() -> None:
    package = cast(
        WorkPackage,
        cast(Any, SimpleNamespace(pause_reason=WorkPackagePauseReason.REPLAN_REQUIRED)),
    )
    with pytest.raises(DomainError, match="failure_context_missing"):
        WorkPackageService._require_failure_pause(package)
