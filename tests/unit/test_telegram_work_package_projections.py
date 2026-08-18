import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.storage.models import WorkPackage
from vuzol.storage.types import StepStatus, WorkPackagePauseReason
from vuzol.telegram.work_package_projections import (
    _environment_delta_lines,
    _package_retry_available,
    _route_provider_label,
)


def test_environment_delta_projection_is_compact_and_escaped() -> None:
    lines = _environment_delta_lines(
        {
            "environment_delta": {
                "upsert_components": [
                    {
                        "key": "api",
                        "label": "API <service>",
                        "technology": "Node & HTTP",
                        "version": "22",
                    },
                    {"key": "worker", "technology": "Python"},
                    "ignored",
                ],
                "remove_components": ["legacy-web", 3],
                "required_capabilities": [
                    {
                        "key": "systemd",
                        "label": "System service",
                        "provisioning": "approval_required",
                    },
                    {
                        "key": "token",
                        "label": "API token",
                        "provisioning": "external_setup",
                    },
                    {"key": "node", "label": "Node", "provisioning": "automatic"},
                    "ignored",
                ],
            }
        }
    )

    assert "+ API &lt;service&gt; · Node &amp; HTTP 22" in lines
    assert "+ worker · Python" in lines
    assert "- legacy-web" in lines
    assert "Approval: System service" in lines
    assert "Setup: API token" in lines


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"environment_delta": []},
        {"environment_delta": {"upsert_components": "bad"}},
    ],
)
def test_environment_delta_projection_ignores_missing_or_invalid_sections(
    body: dict[str, object],
) -> None:
    assert _environment_delta_lines(body) == ()


@pytest.mark.parametrize(
    ("route", "label"),
    [
        (None, None),
        ({}, None),
        ({"profile_id": 7}, None),
        ({"executor_worker_key": "terra-worker"}, "Terra"),
        ({"model_override": "luna-1"}, "Luna"),
        ({"trusted_profile_id": "sol-main"}, "Sol"),
        ({"profile_id": "grok-b"}, "Grok"),
        ({"executor": "kimi"}, "Kimi"),
        ({"executor": "openai-codex"}, "Codex"),
        ({"executor": "custom"}, "custom"),
    ],
)
def test_route_provider_labels(route: object, label: str | None) -> None:
    assert _route_provider_label(route) == label


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


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("pause_reason", "step", "expected"),
    [
        (WorkPackagePauseReason.REPLAN_REQUIRED, object(), False),
        (WorkPackagePauseReason.ITEM_BLOCKED, None, False),
        (
            WorkPackagePauseReason.ITEM_FAILED,
            SimpleNamespace(
                unknown_effects=True,
                attempt_count=1,
                max_attempts=3,
                failure_category="provider_error",
                status=StepStatus.FAILED,
                step_type="validate",
            ),
            False,
        ),
        (
            WorkPackagePauseReason.ITEM_FAILED,
            SimpleNamespace(
                unknown_effects=False,
                attempt_count=1,
                max_attempts=3,
                failure_category="provider_error",
                status=StepStatus.FAILED,
                step_type="validate",
            ),
            True,
        ),
    ],
)
async def test_package_retry_covers_pause_and_step_safety_edges(
    pause_reason: WorkPackagePauseReason, step: object, expected: bool
) -> None:
    session = cast(AsyncSession, cast(Any, AsyncMock()))
    cast(AsyncMock, session.scalar).return_value = step
    package = cast(
        WorkPackage,
        cast(
            Any,
            SimpleNamespace(
                pause_reason=pause_reason,
                last_failure_task_id=uuid.uuid4(),
            ),
        ),
    )

    assert await _package_retry_available(session, package) is expected
