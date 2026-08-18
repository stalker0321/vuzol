import copy
import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.discussion.application import (
    AuthoritativeControlCommand,
    DiscussionPlanApplicationService,
    PackageControlResultCode,
    PackageControlSource,
    _duplicate_result,
    _require_item_scoped_replacement,
    apply_plan_request_in_uow,
)
from vuzol.discussion.domain import (
    DomainError,
    PackageControlAction,
    PlanDraft,
    PlanItemDraft,
    canonical_plan_body,
)
from vuzol.interpretation.discussion import PlanRequestIntent
from vuzol.storage.models import TelegramControlAction
from vuzol.storage.types import ControlActionStatus


def _item(identifier: uuid.UUID, local_id: str, summary: str) -> PlanItemDraft:
    return PlanItemDraft(
        item_id=identifier,
        local_id=local_id,
        summary=summary,
        goal="Keep the change fenced",
        expected_outcome="Only the selected item changes",
        completion_criteria=("Unit tests pass",),
        allowed_scope="src/**",
    )


def _plan(first: uuid.UUID, second: uuid.UUID, *, first_summary: str = "First") -> PlanDraft:
    return PlanDraft(
        title="Plan",
        items=(
            _item(first, "first", first_summary),
            _item(second, "second", "Second"),
        ),
    )


def test_item_scoped_replacement_accepts_only_selected_item() -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    current = canonical_plan_body(_plan(first, second), (first, second))

    _require_item_scoped_replacement(
        _plan(first, second, first_summary="Updated"),
        current_body=current,
        target_item_id=first,
    )


def test_item_scoped_replacement_rejects_every_fence_violation() -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    original = _plan(first, second)
    body = canonical_plan_body(original, (first, second))
    cases: list[tuple[PlanDraft, dict[str, object], uuid.UUID]] = [
        (PlanDraft(title="Other", items=original.items), body, first),
        (original, {"title": "Plan", "items": "bad"}, first),
        (
            PlanDraft(
                title="Plan",
                items=(
                    PlanItemDraft(
                        summary="First",
                        goal="Goal",
                        expected_outcome="Outcome",
                        completion_criteria=("Done",),
                        allowed_scope="src/**",
                    ),
                ),
            ),
            body,
            first,
        ),
        (PlanDraft(title="Plan", items=original.items[:1]), body, first),
        (original, {"title": "Plan", "items": ["bad", body["items"][1]]}, first),
        (_plan(second, first), body, first),
        (
            PlanDraft(
                title="Plan",
                items=(_item(first, "first", "First"), _item(second, "second", "Changed")),
            ),
            body,
            first,
        ),
        (original, body, uuid.uuid4()),
    ]

    for replacement, current, target in cases:
        with pytest.raises(DomainError, match="item_edit_scope_violation"):
            _require_item_scoped_replacement(
                replacement,
                current_body=current,
                target_item_id=target,
            )


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


@pytest.mark.anyio
async def test_plan_application_rejects_unauthorized_or_unfenced_revision() -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    plan = _plan(first, second)
    session_id = uuid.uuid4()
    package_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    snapshot = SimpleNamespace(
        package_id=package_id,
        revision_id=revision_id,
        revision_number=2,
        revision_hash="b" * 64,
        status_generation=3,
    )
    request = SimpleNamespace(project_id="demo", plan_snapshot=snapshot)
    result = SimpleNamespace(
        plan_request=SimpleNamespace(
            intent=PlanRequestIntent.REVISE_DRAFT,
            base_revision_id=revision_id,
            base_revision_hash="b" * 64,
        ),
        should_mutate_plan=True,
        prompt_version="v1",
    )
    uow = MagicMock()
    uow.work_packages.get_package = AsyncMock(
        return_value=SimpleNamespace(
            session_id=session_id,
            project_id="demo",
            head_revision_id=revision_id,
        )
    )
    uow.work_packages.get_fenced_revision = AsyncMock(return_value=SimpleNamespace(id=revision_id))

    cases = [
        (
            SimpleNamespace(plan_request=None, should_mutate_plan=True),
            request,
            PlanRequestIntent.REVISE_DRAFT,
            "plan_mutation_not_authorized",
        ),
        (result, request, PlanRequestIntent.EDIT_ITEM, "edit_session_required"),
        (
            result,
            SimpleNamespace(project_id="demo", plan_snapshot=None),
            PlanRequestIntent.REVISE_DRAFT,
            "stale_revision",
        ),
    ]
    for candidate, candidate_request, intent, code in cases:
        with pytest.raises(DomainError) as error:
            await apply_plan_request_in_uow(
                uow,
                session_id=session_id,
                request=cast(Any, candidate_request),
                result=cast(Any, candidate),
                plan=plan,
                intent=intent,
            )
        assert error.value.code == code

    bad_binding = copy.deepcopy(result)
    bad_binding.plan_request.base_revision_hash = "c" * 64
    with pytest.raises(DomainError, match="stale_revision"):
        await apply_plan_request_in_uow(
            uow,
            session_id=session_id,
            request=cast(Any, request),
            result=cast(Any, bad_binding),
            plan=plan,
            intent=PlanRequestIntent.REVISE_DRAFT,
        )

    uow.work_packages.get_package.return_value.project_id = "other"
    with pytest.raises(DomainError, match="package_context_mismatch"):
        await apply_plan_request_in_uow(
            uow,
            session_id=session_id,
            request=cast(Any, request),
            result=cast(Any, result),
            plan=plan,
            intent=PlanRequestIntent.REVISE_DRAFT,
        )


@pytest.mark.anyio
async def test_plan_application_converts_missing_or_changed_revision_to_stale() -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    plan = _plan(first, second)
    session_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    snapshot = SimpleNamespace(
        package_id=uuid.uuid4(),
        revision_id=revision_id,
        revision_number=1,
        revision_hash="a" * 64,
        status_generation=1,
    )
    request = SimpleNamespace(project_id="demo", plan_snapshot=snapshot)
    result = SimpleNamespace(
        plan_request=SimpleNamespace(
            intent=PlanRequestIntent.REVISE_DRAFT,
            base_revision_id=revision_id,
            base_revision_hash="a" * 64,
        ),
        should_mutate_plan=True,
        prompt_version=None,
    )
    uow = MagicMock()
    package = SimpleNamespace(
        session_id=session_id,
        project_id="demo",
        head_revision_id=revision_id,
    )
    uow.work_packages.get_package = AsyncMock(return_value=package)
    uow.work_packages.get_fenced_revision = AsyncMock(side_effect=LookupError)

    with pytest.raises(DomainError, match="stale_revision"):
        await apply_plan_request_in_uow(
            uow,
            session_id=session_id,
            request=cast(Any, request),
            result=cast(Any, result),
            plan=plan,
            intent=PlanRequestIntent.REVISE_DRAFT,
        )

    uow.work_packages.get_fenced_revision = AsyncMock(return_value=SimpleNamespace(id=uuid.uuid4()))
    with pytest.raises(DomainError, match="stale_revision"):
        await apply_plan_request_in_uow(
            uow,
            session_id=session_id,
            request=cast(Any, request),
            result=cast(Any, result),
            plan=plan,
            intent=PlanRequestIntent.REVISE_DRAFT,
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "intent",
    [PlanRequestIntent.CREATE_DRAFT, PlanRequestIntent.REVISE_DRAFT],
)
async def test_plan_application_routes_create_and_revision_and_enqueues_projection(
    intent: PlanRequestIntent, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = uuid.uuid4(), uuid.uuid4()
    plan = _plan(first, second)
    session_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    package_id = uuid.uuid4()
    snapshot = SimpleNamespace(
        package_id=package_id,
        revision_id=revision_id,
        revision_number=1,
        revision_hash="a" * 64,
        status_generation=1,
    )
    request = SimpleNamespace(
        project_id="demo",
        plan_snapshot=None if intent is PlanRequestIntent.CREATE_DRAFT else snapshot,
    )
    result = SimpleNamespace(
        plan_request=SimpleNamespace(
            intent=intent,
            base_revision_id=revision_id,
            base_revision_hash="a" * 64,
        ),
        should_mutate_plan=True,
        prompt_version="v1",
    )
    revision_result = SimpleNamespace(
        package_id=package_id,
        revision_id=revision_id,
        revision_number=1,
        content_hash="a" * 64,
        status_generation=2,
    )
    service = MagicMock()
    service.create_draft = AsyncMock(return_value=revision_result)
    service.revise_draft = AsyncMock(return_value=revision_result)
    monkeypatch.setattr(
        "vuzol.discussion.application.WorkPackageService", MagicMock(return_value=service)
    )
    uow = MagicMock()
    uow.outbox.enqueue = AsyncMock()
    uow.work_packages.get_package = AsyncMock(
        return_value=SimpleNamespace(
            session_id=session_id,
            project_id="demo",
            head_revision_id=revision_id,
        )
    )
    uow.work_packages.get_fenced_revision = AsyncMock(return_value=SimpleNamespace(id=revision_id))

    applied = await apply_plan_request_in_uow(
        uow,
        session_id=session_id,
        request=cast(Any, request),
        result=cast(Any, result),
        planner_profile="planner",
        plan=plan,
        intent=intent,
    )

    assert cast(Any, applied) is revision_result
    if intent is PlanRequestIntent.CREATE_DRAFT:
        service.create_draft.assert_awaited_once()
        service.revise_draft.assert_not_awaited()
    else:
        service.revise_draft.assert_awaited_once()
        service.create_draft.assert_not_awaited()
    assert uow.outbox.enqueue.await_count == 2


@pytest.mark.anyio
async def test_disabled_discussion_application_fails_before_interpretation() -> None:
    application = DiscussionPlanApplicationService(MagicMock(), enabled=False)

    with pytest.raises(DomainError, match="project_discussion_disabled"):
        await application.apply_plan_request(
            session_id=uuid.uuid4(),
            request=cast(Any, object()),
            interpretation=cast(Any, object()),
        )
