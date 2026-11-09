"""Callback authorization and receipts with transaction fakes, without PostgreSQL."""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import Settings
from vuzol.storage.types import WorkPackageStatus
from vuzol.telegram import controls
from vuzol.telegram.domain import ControlUpdate, IngressStatus, WorkPackageControlUpdate


def _update(**kwargs):
    return ControlUpdate(
        **(
            dict(
                bot_id="main",
                update_id=17,
                callback_query_id="callback-17",
                chat_id=-100,
                user_id=42,
                message_id=23,
                message_thread_id=7,
                action_kind="start",
                task_id=uuid.uuid4(),
            )
            | kwargs
        )
    )


def _service(monkeypatch):
    runtime = SimpleNamespace(
        settings=Settings(environment="test", allowed_chat_ids=(-100,), allowed_user_ids=(42,)),
        registries=MagicMock(),
    )
    uow = MagicMock()
    for name in ("inbox", "outbox", "telegram_actions", "tasks", "session", "work_packages"):
        setattr(uow, name, AsyncMock())
    uow.inbox.receive_once.return_value = (uuid.uuid4(), True)
    uow.telegram_actions.queue_once.return_value = (uuid.uuid4(), True)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=uow)
    context.__aexit__ = AsyncMock(return_value=False)
    constructor = MagicMock(return_value=context)
    monkeypatch.setattr(controls, "UnitOfWork", constructor)
    return controls.TelegramControlService(runtime, MagicMock()), uow, constructor


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["memory_forget", "memory_undo", "lead_resume", "lead_answer"])
@pytest.mark.parametrize("problem", ["message", "thread", "disabled", "project"])
async def test_project_controls_refuse_missing_binding_or_disabled_scope(
    monkeypatch, action, problem
):
    service, uow, constructor = _service(monkeypatch)
    service._runtime.registries.topics.resolve.return_value = SimpleNamespace(
        enabled=problem != "disabled", project_id=None if problem == "project" else "project"
    )
    update = _update(
        action_kind=action,
        message_id=None if problem == "message" else 23,
        message_thread_id=None if problem == "thread" else 7,
    )
    result = await service.accept(update)
    assert result.status is IngressStatus.REJECTED
    assert result.reason == (
        "control_binding_missing" if problem in {"message", "thread"} else "outside_scope"
    )
    constructor.assert_not_called()
    uow.outbox.enqueue.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "problem,reason",
    [
        ("chat", "chat is not allowlisted"),
        ("user", "user is not allowlisted"),
        ("target", "control action requires a persisted target"),
        ("approval", "approval action requires approval_id"),
        ("answer", "answer_required"),
    ],
)
async def test_controls_refuse_unauthorized_or_incomplete_actions_before_transaction(
    monkeypatch, problem, reason
):
    service, _, constructor = _service(monkeypatch)
    service._runtime.registries.topics.resolve.return_value = SimpleNamespace(
        enabled=True, project_id="project"
    )
    result = await service.accept(
        _update(
            chat_id=-999 if problem == "chat" else -100,
            user_id=99 if problem == "user" else 42,
            task_id=None if problem == "target" else uuid.uuid4(),
            action_kind="approve"
            if problem == "approval"
            else "lead_answer"
            if problem == "answer"
            else "start",
        )
    )
    assert result.status is IngressStatus.REJECTED
    assert result.reason == reason
    constructor.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("message_id", [None, 23])
async def test_secret_cancellation_deletes_only_its_bound_message(monkeypatch, message_id):
    service, uow, _ = _service(monkeypatch)
    cancel = AsyncMock(return_value=True)
    monkeypatch.setattr(controls, "cancel_request", cancel)
    request_id = uuid.uuid4()
    result = await service.accept(
        _update(
            action_kind="secret_cancel",
            task_id=None,
            secret_request_id=request_id,
            message_id=message_id,
        )
    )
    assert result.status is IngressStatus.HANDLED and result.reason == "secret_cancelled"
    cancel.assert_awaited_once_with(uow.session, request_id, user_id=42)
    assert uow.inbox.mark_processed.await_args.kwargs["entity_id"] == request_id
    if message_id:
        queued = uow.outbox.enqueue.await_args.kwargs
        assert queued["operation_type"] == "delete_message"
        assert queued["entity_id"] == request_id
        assert queued["payload"]["message_id"] == message_id
        assert queued["payload"]["chat_id"] == -100
    else:
        uow.outbox.enqueue.assert_not_awaited()
    uow.telegram_actions.queue_once.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("missing_id", [False, True])
async def test_secret_cancellation_refuses_unavailable_request_without_delete(
    monkeypatch, missing_id
):
    service, uow, _ = _service(monkeypatch)
    cancel = AsyncMock(return_value=False)
    monkeypatch.setattr(controls, "cancel_request", cancel)
    result = await service.accept(
        _update(action_kind="secret_cancel", secret_request_id=None if missing_id else uuid.uuid4())
    )
    assert result.status is IngressStatus.REJECTED
    assert result.reason == "secret request is unavailable"
    uow.outbox.enqueue.assert_not_awaited()
    uow.inbox.mark_processed.assert_not_awaited()
    assert cancel.await_count == int(not missing_id)


@pytest.mark.anyio
@pytest.mark.parametrize("created", [False, True])
async def test_model_preference_callbacks_apply_once_and_return_action_receipt(
    monkeypatch, created
):
    service, uow, _ = _service(monkeypatch)
    action_id = uuid.uuid4()
    uow.telegram_actions.queue_once.return_value = (action_id, created)
    service._project_model = SimpleNamespace(apply=AsyncMock())
    update = _update(
        action_kind="project_model_select_effort",
        task_id=None,
        preference_revision=4,
        preference_worker="sol",
        preference_profile_id="pi-a",
        preference_effort="high",
    )
    result = await service.accept(update)
    assert result.action_id == action_id
    assert result.status is (IngressStatus.CREATED if created else IngressStatus.DUPLICATE)
    assert service._project_model.apply.await_count == int(created)
    if created:
        service._project_model.apply.assert_awaited_once_with(
            uow.session, update, action_id=action_id
        )
    action = uow.telegram_actions.queue_once.await_args.args[0]
    assert action.requested_by_user_id == 42 and action.task_id is None
    assert action.payload == {
        "preference_revision": 4,
        "preference_worker": "sol",
        "preference_profile_id": "pi-a",
        "preference_effort": "high",
        "message_thread_id": 7,
    }
    uow.outbox.enqueue.assert_not_awaited()
    assert uow.inbox.mark_processed.await_args.kwargs["entity_id"] == action_id


@pytest.mark.anyio
@pytest.mark.parametrize(
    "stored", [None, {}, {"outcome": "bad"}, {"outcome": {"status": "applied"}}]
)
async def test_duplicate_task_action_returns_prior_outcome_without_requeue(monkeypatch, stored):
    service, uow, _ = _service(monkeypatch)
    action_id = uuid.uuid4()
    uow.telegram_actions.queue_once.return_value = (action_id, False)
    uow.session.get.return_value = None if stored is None else SimpleNamespace(payload=stored)
    result = await service.accept(_update())
    assert result.status is IngressStatus.DUPLICATE
    assert result.action_id == action_id
    assert result.outcome == (
        {"status": "applied"} if stored == {"outcome": {"status": "applied"}} else None
    )
    uow.outbox.enqueue.assert_not_awaited()
    uow.inbox.mark_processed.assert_awaited_once()


@pytest.mark.anyio
async def test_missing_approval_is_rejected_without_enqueuing_action(monkeypatch):
    service, uow, _ = _service(monkeypatch)
    approval_id = uuid.uuid4()
    uow.session.get.return_value = None
    result = await service.accept(
        _update(action_kind="approve", task_id=None, approval_id=approval_id)
    )
    assert result.status is IngressStatus.REJECTED
    assert result.reason == f"approval not found: {approval_id}"
    uow.telegram_actions.queue_once.assert_not_awaited()
    uow.outbox.enqueue.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("case", ["missing", "discarded", "available"])
async def test_stale_package_card_repost_requires_a_live_package(monkeypatch, case):
    service, uow, _ = _service(monkeypatch)
    package_id = uuid.uuid4()
    update = WorkPackageControlUpdate(
        bot_id="main",
        update_id=17,
        callback_query_id="callback-17",
        callback_data=f"v1:wp:A:{package_id.hex}:1:abcdef12",
        chat_id=-100,
        user_id=42,
        message_id=23,
        message_thread_id=7,
    )
    if case == "missing":
        uow.work_packages.get_package.side_effect = LookupError("missing")
    else:
        uow.work_packages.get_package.return_value = SimpleNamespace(
            status=WorkPackageStatus.DISCARDED if case == "discarded" else WorkPackageStatus.DRAFT
        )
    assert await service._repost_stale_card(update) is (case == "available")
    if case == "available":
        assert uow.outbox.enqueue.await_args.kwargs["operation_type"] == "repost_plan"
        assert uow.outbox.enqueue.await_args.kwargs["entity_id"] == package_id
    else:
        uow.outbox.enqueue.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "problem",
    ["missing_task", "thread", "owner", "missing_link", "link_revision", "stale", "current"],
)
async def test_versioned_task_controls_require_scope_and_matching_card_revision(
    monkeypatch, problem
):
    service, uow, _ = _service(monkeypatch)
    task_id = uuid.uuid4()
    task = SimpleNamespace(
        id=task_id,
        version=5 if problem == "stale" else 4,
        user_id=99 if problem == "owner" else 42,
        source_chat_id=-100,
        source_thread_id=None if problem == "thread" else 7,
    )
    uow.session.get.return_value = None if problem == "missing_task" else task
    uow.session.scalar.return_value = (
        None
        if problem == "missing_link"
        else SimpleNamespace(projection_revision=3 if problem == "link_revision" else 4)
    )
    snapshot = AsyncMock()
    monkeypatch.setattr("vuzol.telegram.projections.enqueue_task_control_snapshot", snapshot)
    result = await service.accept(_update(task_id=task_id, expected_task_version=4))
    if problem == "current":
        assert result.status is IngressStatus.CREATED
        action = uow.telegram_actions.queue_once.await_args.args[0]
        assert action.payload == {"expected_task_version": 4}
        assert uow.outbox.enqueue.await_args.kwargs["operation_type"] == "start"
    else:
        assert result.status is IngressStatus.REJECTED
        assert result.reason == (
            "task_retry_scope_mismatch"
            if problem in {"missing_task", "thread", "owner"}
            else "task_retry_stale"
            if problem == "stale"
            else "task_retry_message_mismatch"
        )
        uow.telegram_actions.queue_once.assert_not_awaited()
        uow.outbox.enqueue.assert_not_awaited()
    if problem == "stale":
        snapshot.assert_awaited_once_with(
            uow.session,
            task=task,
            chat_id=-100,
            thread_id=7,
            user_id=42,
            key="task:control-stale:callback-17",
        )
        uow.inbox.mark_processed.assert_awaited_once()
    else:
        snapshot.assert_not_awaited()


@pytest.mark.anyio
async def test_rejects_retry_without_fenced_version(monkeypatch):
    service, uow, _ = _service(monkeypatch)
    uow.session.get.return_value = SimpleNamespace(
        user_id=42, source_chat_id=-100, source_thread_id=7
    )
    result = await service.accept(_update(action_kind="retry"))
    assert result.status is IngressStatus.REJECTED
    assert result.reason == "task_retry_version_required"
    uow.telegram_actions.queue_once.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("case", ["missing", "unlinked", "linked"])
async def test_status_card_revision_combines_package_version_when_present(monkeypatch, case):
    _service_instance, uow, _ = _service(monkeypatch)
    uow.session.scalar.return_value = (
        None if case == "unlinked" else SimpleNamespace(work_package_id=uuid.uuid4())
    )
    uow.session.get.return_value = SimpleNamespace(version=3) if case == "linked" else None
    assert await controls._status_card_revision(
        uow.session, SimpleNamespace(id=uuid.uuid4(), version=4)
    ) == (40003 if case == "linked" else 4)
    if case == "unlinked":
        uow.session.get.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kind,operation",
    [
        ("I", "render_detail"),
        ("E", "open_edit"),
        ("C", "clear_detail"),
        ("G", "render_plan"),
        ("L", "request_replan"),
        ("U", "continue_discussion"),
    ],
)
async def test_package_navigation_applies_bound_revision_and_queues_only_needed_projection(
    monkeypatch, kind, operation
):
    from vuzol.telegram.work_packages import WorkPackageCallback, WorkPackageCallbackKind

    service, uow, _ = _service(monkeypatch)
    package_id, revision_id = uuid.uuid4(), uuid.uuid4()
    revision = SimpleNamespace(
        id=revision_id,
        work_package_id=package_id,
        revision_number=2,
        content_hash="abcdef12" + "a" * 56,
    )
    package = SimpleNamespace(head_revision_id=revision_id, version=7, managed_goal_id=None)
    uow.telegram_links = SimpleNamespace(
        resolve_work_package_control=AsyncMock(
            return_value=(package_id, revision_id, 6, "work_package_plan")
        )
    )
    uow.work_packages.get_package.return_value = package
    uow.work_packages.get_revision.return_value = revision
    uow.session.scalars.return_value = SimpleNamespace(all=lambda: [uuid.UUID(int=1)])
    package_service = SimpleNamespace(
        **{
            name: AsyncMock()
            for name in (
                "set_open_detail",
                "open_edit_session",
                "clear_open_detail",
                "request_replan",
                "close_edit_session",
            )
        }
    )
    monkeypatch.setattr(controls, "WorkPackageService", lambda _uow: package_service)
    overrides = SimpleNamespace(arm=AsyncMock())
    service._continue_discussion_overrides = overrides
    callback = WorkPackageCallback(
        WorkPackageCallbackKind(kind),
        package_id,
        2,
        "abcdef12",
        1 if kind in {"I", "E", "G"} else None,
    )
    update = WorkPackageControlUpdate(
        bot_id="main",
        update_id=17,
        callback_query_id="callback-17",
        callback_data="unused",
        chat_id=-100,
        user_id=42,
        message_id=23,
        message_thread_id=7,
    )
    result = await service._apply_non_mutating_package_callback(update, callback)
    assert result.status is IngressStatus.HANDLED and result.reason == operation
    if kind == "I":
        package_service.set_open_detail.assert_awaited_once_with(
            package_id=package_id, revision_number=2, h8="abcdef12", ordinal=1
        )
    elif kind == "E":
        package_service.open_edit_session.assert_awaited_once_with(
            package_id=package_id, revision_number=2, h8="abcdef12", ordinal=1, user_id=42
        )
    elif kind == "C":
        package_service.clear_open_detail.assert_awaited_once_with(package_id=package_id)
    elif kind == "L":
        package_service.request_replan.assert_awaited_once_with(
            package_id=package_id,
            revision_number=2,
            h8="abcdef12",
            expected_status_generation=7,
            user_id=42,
        )
    elif kind == "U":
        package_service.close_edit_session.assert_awaited_once_with(
            edit_session_id=uuid.UUID(int=1), user_id=42
        )
        assert overrides.arm.await_args.kwargs["user_id"] == 42
    if kind in {"G", "L"}:
        queued = uow.outbox.enqueue.await_args.kwargs
        assert queued["operation_type"] == operation and queued["entity_id"] == package_id
        assert queued["payload"] == (
            {"package_id": str(package_id), "page": 1}
            if kind == "G"
            else {"package_id": str(package_id)}
        )
    else:
        uow.outbox.enqueue.assert_not_awaited()
    uow.inbox.mark_processed.assert_awaited_once()


@pytest.mark.anyio
@pytest.mark.parametrize("owner", [None, 99])
async def test_managed_package_discussion_refuses_missing_or_foreign_goal(monkeypatch, owner):
    from vuzol.discussion.domain import DomainError
    from vuzol.telegram.work_packages import WorkPackageCallback, WorkPackageCallbackKind

    service, uow, _ = _service(monkeypatch)
    package_id, revision_id = uuid.uuid4(), uuid.uuid4()
    uow.telegram_links = SimpleNamespace(
        resolve_work_package_control=AsyncMock(
            return_value=(package_id, revision_id, 7, "work_package_plan")
        )
    )
    uow.work_packages.get_package.return_value = SimpleNamespace(
        head_revision_id=revision_id, version=7, managed_goal_id=uuid.uuid4()
    )
    uow.work_packages.get_revision.return_value = SimpleNamespace(
        id=revision_id,
        work_package_id=package_id,
        revision_number=2,
        content_hash="abcdef12" + "a" * 56,
    )
    uow.session.scalars.return_value = SimpleNamespace(all=lambda: [])
    uow.session.get.return_value = None if owner is None else SimpleNamespace(owner_user_id=owner)
    monkeypatch.setattr(controls, "WorkPackageService", MagicMock())
    callback = WorkPackageCallback(
        WorkPackageCallbackKind.CONTINUE_DISCUSSION, package_id, 2, "abcdef12"
    )
    update = WorkPackageControlUpdate(
        bot_id="main",
        update_id=17,
        callback_query_id="callback-17",
        callback_data="unused",
        chat_id=-100,
        user_id=42,
        message_id=23,
        message_thread_id=7,
    )
    with pytest.raises(DomainError, match="outside_scope"):
        await service._apply_non_mutating_package_callback(update, callback)
    uow.outbox.enqueue.assert_not_awaited()
    uow.inbox.mark_processed.assert_not_awaited()


@pytest.mark.anyio
async def test_malformed_stale_package_callback_is_not_reposted(monkeypatch):
    service, uow, constructor = _service(monkeypatch)
    update = WorkPackageControlUpdate(
        bot_id="main",
        update_id=17,
        callback_query_id="callback-17",
        callback_data="invalid",
        chat_id=-100,
        user_id=42,
        message_id=23,
    )
    assert await service._repost_stale_card(update) is False
    constructor.assert_not_called()
    uow.outbox.enqueue.assert_not_awaited()
