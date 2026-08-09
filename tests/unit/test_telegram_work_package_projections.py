import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import WorkPackage
from vuzol.storage.types import WorkPackagePauseReason
from vuzol.telegram.work_package_projections import _package_retry_available


@pytest.mark.anyio
async def test_package_retry_is_off_without_a_blocked_item() -> None:
    session = cast(AsyncSession, cast(Any, AsyncMock()))
    package = cast(
        WorkPackage,
        cast(
            Any,
            SimpleNamespace(
                pause_reason=WorkPackagePauseReason.REPLAN_REQUIRED,
                last_failure_task_id=None,
            ),
        ),
    )

    assert not await _package_retry_available(session, package)
    cast(AsyncMock, session.scalar).assert_not_awaited()


@pytest.mark.anyio
async def test_package_retry_uses_shared_safe_retry_policy() -> None:
    step = SimpleNamespace(
        unknown_effects=False,
        attempt_count=3,
        max_attempts=3,
        failure_category="quota_exhausted",
    )
    session = cast(AsyncSession, cast(Any, AsyncMock()))
    cast(AsyncMock, session.scalar).return_value = step
    package = cast(
        WorkPackage,
        cast(
            Any,
            SimpleNamespace(
                pause_reason=WorkPackagePauseReason.ITEM_BLOCKED,
                last_failure_task_id=uuid.uuid4(),
            ),
        ),
    )

    assert await _package_retry_available(session, package)
    cast(AsyncMock, session.scalar).assert_awaited_once()
