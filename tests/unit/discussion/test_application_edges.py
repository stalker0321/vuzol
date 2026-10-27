import copy
import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    PackageControlResultCode,
    PackageControlSource,
    _duplicate_result,
)
from vuzol.discussion.domain import (
    DomainError,
    PackageControlAction,
)
from vuzol.storage.models import TelegramControlAction
from vuzol.storage.types import ControlActionStatus


def _command() -> AuthoritativeControlCommand:
    return AuthoritativeControlCommand(
        action=PackageControlAction.APPROVE,
        package_id=uuid.uuid4(),
        plan_revision_number=1,
        h8="a" * 8,
        expected_status_generation=1,
        user_id=42,
        source=PackageControlSource.TELEGRAM_CALLBACK,
        external_idempotency_key="callback-1",
    )


def _persisted(command: AuthoritativeControlCommand, payload: dict[str, object]) -> Any:
    return SimpleNamespace(
        id=uuid.uuid4(),
        action_kind=f"work_package.{command.action.value}",
        requested_by_user_id=command.user_id,
        payload={
            "command": payload,
            "outcome": {
                "code": PackageControlResultCode.APPLIED.value,
                "status_generation": 2,
                "revision_id": None,
            },
        },
        status=ControlActionStatus.PROCESSED,
    )


def test_duplicate_control_result_restores_optional_revision() -> None:
    command = _command()
    payload: dict[str, object] = {"action": "approve"}
    persisted = _persisted(command, payload)

    result = _duplicate_result(cast(TelegramControlAction, persisted), payload, command)
    revision_id = uuid.uuid4()
    persisted.payload["outcome"]["revision_id"] = str(revision_id)
    with_revision = _duplicate_result(cast(TelegramControlAction, persisted), payload, command)

    assert result.duplicate and result.revision_id is None
    assert with_revision.revision_id == revision_id


def test_duplicate_control_result_rejects_conflicts_and_corrupt_outcomes() -> None:
    command = _command()
    payload: dict[str, object] = {"action": "approve"}
    base = _persisted(command, payload)
    conflicts = []
    for attribute, value in (
        ("action_kind", "work_package.discard"),
        ("requested_by_user_id", 7),
    ):
        candidate = copy.deepcopy(base)
        setattr(candidate, attribute, value)
        conflicts.append(candidate)
    wrong_payload = copy.deepcopy(base)
    wrong_payload.payload["command"] = {"action": "discard"}
    conflicts.append(wrong_payload)
    for persisted in conflicts:
        with pytest.raises(DomainError, match="idempotency_conflict"):
            _duplicate_result(cast(TelegramControlAction, persisted), payload, command)

    queued = copy.deepcopy(base)
    queued.status = ControlActionStatus.QUEUED
    missing_outcome = copy.deepcopy(base)
    missing_outcome.payload["outcome"] = []
    for persisted in (queued, missing_outcome):
        with pytest.raises(DomainError, match="idempotency_incomplete"):
            _duplicate_result(cast(TelegramControlAction, persisted), payload, command)

    for outcome in (
        {},
        {"code": "bad", "status_generation": 1},
        {"code": "applied", "status_generation": object()},
        {"code": "applied", "status_generation": 1, "revision_id": "bad"},
    ):
        corrupt = copy.deepcopy(base)
        corrupt.payload["outcome"] = outcome
        with pytest.raises(DomainError, match="idempotency_outcome_invalid"):
            _duplicate_result(cast(TelegramControlAction, corrupt), payload, command)
