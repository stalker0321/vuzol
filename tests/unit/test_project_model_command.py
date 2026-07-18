"""Telegram /model command parsing and picker keyboards."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from vuzol.projects.executor_preference import (
    ExecutorWorkerKey,
    WorkerOption,
    auto_callback_data,
    effort_callback_data,
    worker_callback_data,
)
from vuzol.telegram.adapter import control_update
from vuzol.telegram.domain import ControlUpdate, IngressStatus
from vuzol.telegram.layout import is_model_command
from vuzol.telegram.model_command import (
    PROJECT_MODEL_PICKER_ROLE,
    effort_keyboard,
    worker_keyboard,
)


class _Message:
    def __init__(self, chat_id: int, thread_id: int) -> None:
        self.chat = type("Chat", (), {"id": chat_id})()
        self.message_thread_id = thread_id


class _Query:
    def __init__(self, data: str, chat_id: int = -100, thread_id: int = 42) -> None:
        self.id = "cbq-1"
        self.data = data
        self.message = _Message(chat_id, thread_id)


class _User:
    id = 7


class _Update:
    def __init__(self, data: str) -> None:
        self.update_id = 99
        self.callback_query = _Query(data)
        self.effective_user = _User()


def test_is_model_command() -> None:
    assert is_model_command("/model")
    assert is_model_command("/model@vuzol_bot")
    assert not is_model_command("/model sol")
    assert not is_model_command("/update")
    assert not is_model_command(None)


def test_worker_and_effort_keyboards() -> None:
    workers = (
        WorkerOption(ExecutorWorkerKey.SOL, "Sol", True),
        WorkerOption(ExecutorWorkerKey.GROK, "Grok", False),
    )
    keyboard = worker_keyboard(revision=3, workers=workers)
    assert keyboard[0] == (("Routing (auto)", auto_callback_data(3)),)
    assert ("Sol", worker_callback_data(3, ExecutorWorkerKey.SOL)) in keyboard[1]
    effort = effort_keyboard(revision=3, worker=ExecutorWorkerKey.TERRA)
    assert any(
        data == effort_callback_data(3, ExecutorWorkerKey.TERRA, "high")
        for row in effort
        for _, data in row
    )


def test_control_update_parses_model_callbacks() -> None:
    auto = control_update(_Update("v1:pm:a:2"), "bot")  # type: ignore[arg-type]
    assert isinstance(auto, ControlUpdate)
    assert auto.action_kind == "project_model_select_auto"
    assert auto.preference_revision == 2
    assert auto.message_thread_id == 42

    worker = control_update(_Update("v1:pm:w:2:sol"), "bot")  # type: ignore[arg-type]
    assert worker is not None
    assert worker.action_kind == "project_model_select_worker"
    assert worker.preference_worker == "sol"

    effort = control_update(_Update("v1:pm:e:2:terra:high"), "bot")  # type: ignore[arg-type]
    assert effort is not None
    assert effort.action_kind == "project_model_select_effort"
    assert effort.preference_worker == "terra"
    assert effort.preference_effort == "high"

    assert control_update(_Update("v1:pm:e:2:high"), "bot") is None  # type: ignore[arg-type]


@pytest.mark.anyio
async def test_accept_message_routes_model_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.config.models import TopicConfig, TopicKind
    from vuzol.telegram.domain import MessageUpdate
    from vuzol.telegram.ingress import TelegramIngressService

    update = MessageUpdate(
        bot_id="main",
        update_id=11,
        chat_id=-100,
        message_thread_id=20,
        message_id=30,
        user_id=42,
        text="/model",
    )
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=20,
        kind=TopicKind.PROJECT,
        project_id="bill-buddy",
        display_name="Bill Buddy",
        accepts_new_tasks=True,
        default_workflow="adaptive_task",
        enabled=True,
    )
    runtime = MagicMock()
    runtime.registries.topics.resolve.return_value = topic
    service = TelegramIngressService(runtime, MagicMock())
    handled = AsyncMock(return_value=MagicMock(status=IngressStatus.HANDLED))
    monkeypatch.setattr(service, "_handle_model_command", handled)
    monkeypatch.setattr("vuzol.telegram.ingress.authorize", lambda *a, **k: None)
    monkeypatch.setattr("vuzol.telegram.ingress.validate_message", lambda *a, **k: None)
    result = await service.accept_message(update)
    assert result.status is IngressStatus.HANDLED
    handled.assert_awaited_once_with(update, topic)


@pytest.mark.anyio
async def test_accept_message_rejects_model_outside_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.config.models import TopicConfig, TopicKind
    from vuzol.telegram.domain import MessageUpdate
    from vuzol.telegram.ingress import TelegramIngressService

    update = MessageUpdate(
        bot_id="main",
        update_id=12,
        chat_id=-100,
        message_thread_id=5,
        message_id=31,
        user_id=42,
        text="/model",
    )
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=5,
        kind=TopicKind.TASK_DASHBOARD,
        accepts_new_tasks=False,
        default_workflow="simple_model_task",
        enabled=True,
    )
    runtime = MagicMock()
    runtime.registries.topics.resolve.return_value = topic
    service = TelegramIngressService(runtime, MagicMock())
    monkeypatch.setattr("vuzol.telegram.ingress.authorize", lambda *a, **k: None)
    monkeypatch.setattr("vuzol.telegram.ingress.validate_message", lambda *a, **k: None)
    result = await service.accept_message(update)
    assert result.status is IngressStatus.REJECTED
    assert result.reason is not None
    assert "/model" in result.reason


@pytest.mark.anyio
async def test_handle_model_command_enqueues_picker_and_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.config.models import TopicConfig, TopicKind
    from vuzol.telegram.domain import MessageUpdate
    from vuzol.telegram.ingress import TelegramIngressService

    update = MessageUpdate(
        bot_id="main",
        update_id=13,
        chat_id=-100,
        message_thread_id=20,
        message_id=88,
        user_id=42,
        text="/model",
    )
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=20,
        kind=TopicKind.PROJECT,
        project_id="bill-buddy",
        display_name="Bill Buddy",
        accepts_new_tasks=True,
        default_workflow="adaptive_task",
        enabled=True,
    )
    inbox_id = uuid4()
    enqueued: list[dict[str, object]] = []
    session = MagicMock()
    session.add = MagicMock()

    class FakeOutbox:
        async def enqueue(self, **kwargs: object) -> object:
            enqueued.append(kwargs)
            return uuid4()

    class FakeInbox:
        async def receive_once(self, **kwargs: object) -> tuple[object, bool]:
            del kwargs
            return inbox_id, True

        async def mark_processed(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    class FakeUow:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.inbox = FakeInbox()
            self.outbox = FakeOutbox()
            self.session = session

        async def __aenter__(self) -> "FakeUow":
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

    async def fake_enqueue_worker_picker(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr("vuzol.telegram.ingress.UnitOfWork", FakeUow)
    monkeypatch.setattr("vuzol.telegram.ingress.enqueue_worker_picker", fake_enqueue_worker_picker)
    service = TelegramIngressService(MagicMock(), MagicMock())
    result = await service._handle_model_command(update, topic)
    assert result.status is IngressStatus.HANDLED
    assert len(enqueued) == 1
    assert enqueued[0]["operation_type"] == "delete_message"
    assert enqueued[0]["payload"]["message_id"] == 88  # type: ignore[index]


def test_prepare_model_picker_delivery() -> None:
    from vuzol.telegram.delivery import DeliveryAction, _prepare_project_model_message

    item = SimpleNamespace(
        payload={
            "role": PROJECT_MODEL_PICKER_ROLE,
            "chat_id": -100,
            "message_thread_id": 20,
            "html": "<b>Model</b>",
            "callback_buttons": [
                [["Routing (auto)", "v1:pm:a:1"]],
                [["Sol", "v1:pm:w:1:sol"], ["Grok", "v1:pm:w:1:grok"]],
            ],
        }
    )
    prepared = _prepare_project_model_message(item)  # type: ignore[arg-type]
    assert prepared.action is DeliveryAction.SEND_MODEL_PICKER
    assert prepared.chat_id == -100
    assert prepared.thread_id == 20
    assert prepared.callback_buttons[0][0][1] == "v1:pm:a:1"


@pytest.mark.anyio
async def test_project_model_controller_auto_and_effort_and_grok() -> None:
    from pathlib import Path

    from vuzol.config import (
        Capability,
        ConfigurationBundle,
        CostClass,
        LaunchMode,
        ProfileRegistry,
        ProjectConfig,
        ProjectRegistry,
        ProviderProfileConfig,
        ProviderRole,
        RuntimeConfiguration,
        SandboxProfileConfig,
        SandboxRegistry,
        TopicConfig,
        TopicKind,
        TopicRegistry,
    )
    from vuzol.telegram.domain import ControlUpdate
    from vuzol.telegram.model_command import ModelPickerStage, ProjectModelController

    sandbox = SandboxProfileConfig.model_validate(
        {
            "id": "project-default",
            "image": "registry.example/vuzol-sandbox@sha256:" + ("0" * 64),
            "enabled": False,
        }
    )
    project = ProjectConfig.model_validate(
        {
            "id": "bill-buddy",
            "display_name": "Bill Buddy",
            "repository_path": "bill-buddy",
            "default_branch": "main",
            "allowed_capabilities": frozenset(
                {
                    Capability.REPOSITORY_READ,
                    Capability.CODE_EDIT,
                    Capability.GIT,
                    Capability.PROJECT_SHELL,
                }
            ),
            "sandbox_profile": "project-default",
            "enabled": False,
        }
    )
    projects = ProjectRegistry((project,), repository_root=Path("/tmp"))  # noqa: S108
    topic = TopicConfig(
        chat_id=-100,
        message_thread_id=20,
        kind=TopicKind.PROJECT,
        project_id="bill-buddy",
        display_name="Bill Buddy",
        default_workflow="adaptive_task",
        enabled=True,
    )
    profiles = (
        ProviderProfileConfig.model_validate(
            {
                "id": "codex-subscription-prod",
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "model_reasoning_effort": "medium",
                "launch_mode": LaunchMode.CLI,
                "credential_required": False,
                "capabilities": frozenset(
                    {
                        Capability.REPOSITORY_READ,
                        Capability.CODE_EDIT,
                        Capability.GIT,
                        Capability.PROJECT_SHELL,
                    }
                ),
                "concurrency_limit": 1,
                "cost_class": CostClass.STRONG,
                "roles": frozenset({ProviderRole.EXECUTOR}),
                "routing_priority": 200,
                "supported_task_types": frozenset({"coding"}),
                "fallback_profile_ids": (),
                "sandbox_required": True,
                "runtime_identity": "vuzol-executor",
                "state_directory": Path("/var/lib/vuzol-provider-state/codex"),
                "enabled": True,
            }
        ),
        ProviderProfileConfig.model_validate(
            {
                "id": "grok-subscription-a",
                "provider": "grok",
                "model": "grok-build",
                "launch_mode": LaunchMode.CLI,
                "credential_required": False,
                "capabilities": frozenset(
                    {
                        Capability.REPOSITORY_READ,
                        Capability.CODE_EDIT,
                        Capability.GIT,
                        Capability.PROJECT_SHELL,
                    }
                ),
                "concurrency_limit": 1,
                "cost_class": CostClass.STRONG,
                "roles": frozenset({ProviderRole.EXECUTOR}),
                "routing_priority": 210,
                "supported_task_types": frozenset({"coding"}),
                "fallback_profile_ids": (),
                "sandbox_required": True,
                "runtime_identity": "vuzol-grok-a",
                "state_directory": Path("/var/lib/vuzol-provider-state/grok-a"),
                "enabled": True,
            }
        ),
    )
    registries = ConfigurationBundle(
        projects=projects,
        profiles=ProfileRegistry(profiles),
        topics=TopicRegistry((topic,), projects=projects),
        sandboxes=SandboxRegistry((sandbox,)),
        revision="test",
    )
    runtime = MagicMock(spec=RuntimeConfiguration)
    runtime.registries = registries
    controller = ProjectModelController(runtime)

    class Row:
        def __init__(self) -> None:
            self.project_id = "bill-buddy"
            self.mode = "auto"
            self.worker_key: str | None = None
            self.reasoning_effort: str | None = None
            self.revision = 1
            self.updated_by_user_id: int | None = None

    row = Row()
    session = MagicMock()
    session.get = AsyncMock(return_value=row)
    session.flush = AsyncMock()
    session.add = MagicMock()

    auto = await controller.apply(
        session,
        ControlUpdate(
            bot_id="main",
            update_id=1,
            callback_query_id="cb-auto",
            chat_id=-100,
            user_id=7,
            message_thread_id=20,
            action_kind="project_model_select_auto",
            preference_revision=1,
        ),
        action_id=uuid4(),
    )
    assert auto.stage is ModelPickerStage.CONFIRM
    assert row.mode == "auto"
    assert row.revision == 2

    row.revision = 2
    effort_stage = await controller.apply(
        session,
        ControlUpdate(
            bot_id="main",
            update_id=2,
            callback_query_id="cb-worker",
            chat_id=-100,
            user_id=7,
            message_thread_id=20,
            action_kind="project_model_select_worker",
            preference_revision=2,
            preference_worker="sol",
        ),
        action_id=uuid4(),
    )
    assert effort_stage.stage is ModelPickerStage.EFFORT
    assert row.revision == 2  # not committed until effort chosen

    pin = await controller.apply(
        session,
        ControlUpdate(
            bot_id="main",
            update_id=3,
            callback_query_id="cb-effort",
            chat_id=-100,
            user_id=7,
            message_thread_id=20,
            action_kind="project_model_select_effort",
            preference_revision=2,
            preference_worker="sol",
            preference_effort="high",
        ),
        action_id=uuid4(),
    )
    assert pin.stage is ModelPickerStage.CONFIRM
    assert row.mode == "pin"
    assert row.worker_key == "sol"
    assert row.reasoning_effort == "high"
    assert row.revision == 3

    row.revision = 3
    grok = await controller.apply(
        session,
        ControlUpdate(
            bot_id="main",
            update_id=4,
            callback_query_id="cb-grok",
            chat_id=-100,
            user_id=7,
            message_thread_id=20,
            action_kind="project_model_select_worker",
            preference_revision=3,
            preference_worker="grok",
        ),
        action_id=uuid4(),
    )
    assert grok.stage is ModelPickerStage.CONFIRM
    assert row.worker_key == "grok"
    assert row.reasoning_effort is None
    assert session.add.call_count >= 3


@pytest.mark.anyio
async def test_project_model_controller_rejects_incomplete_and_non_project() -> None:
    from pathlib import Path

    from vuzol.config import (
        Capability,
        ConfigurationBundle,
        ProfileRegistry,
        ProjectConfig,
        ProjectRegistry,
        RuntimeConfiguration,
        SandboxProfileConfig,
        SandboxRegistry,
        TopicConfig,
        TopicKind,
        TopicRegistry,
    )
    from vuzol.projects.executor_preference import ExecutorPreferenceError
    from vuzol.telegram.domain import ControlUpdate
    from vuzol.telegram.model_command import ProjectModelController

    sandbox = SandboxProfileConfig.model_validate(
        {
            "id": "project-default",
            "image": "registry.example/vuzol-sandbox@sha256:" + ("0" * 64),
            "enabled": False,
        }
    )
    project = ProjectConfig.model_validate(
        {
            "id": "bill-buddy",
            "display_name": "Bill Buddy",
            "repository_path": "bill-buddy",
            "default_branch": "main",
            "allowed_capabilities": frozenset({Capability.REPOSITORY_READ}),
            "sandbox_profile": "project-default",
            "enabled": False,
        }
    )
    projects = ProjectRegistry((project,), repository_root=Path("/tmp"))  # noqa: S108
    dashboard = TopicConfig(
        chat_id=-100,
        message_thread_id=5,
        kind=TopicKind.TASK_DASHBOARD,
        accepts_new_tasks=False,
        default_workflow="simple_model_task",
        enabled=True,
    )
    registries = ConfigurationBundle(
        projects=projects,
        profiles=ProfileRegistry(()),
        topics=TopicRegistry((dashboard,), projects=projects),
        sandboxes=SandboxRegistry((sandbox,)),
        revision="test",
    )
    runtime = MagicMock(spec=RuntimeConfiguration)
    runtime.registries = registries
    controller = ProjectModelController(runtime)
    session = MagicMock()
    with pytest.raises(ExecutorPreferenceError, match="incomplete"):
        await controller.apply(
            session,
            ControlUpdate(
                bot_id="main",
                update_id=1,
                callback_query_id="cb",
                chat_id=-100,
                user_id=1,
                action_kind="project_model_select_auto",
                preference_revision=1,
            ),
            action_id=uuid4(),
        )
    with pytest.raises(ExecutorPreferenceError, match="project topic"):
        await controller.apply(
            session,
            ControlUpdate(
                bot_id="main",
                update_id=2,
                callback_query_id="cb2",
                chat_id=-100,
                user_id=1,
                message_thread_id=5,
                action_kind="project_model_select_auto",
                preference_revision=1,
            ),
            action_id=uuid4(),
        )


@pytest.mark.anyio
async def test_enqueue_worker_picker_creates_outbox() -> None:
    from pathlib import Path

    from vuzol.config import (
        Capability,
        ConfigurationBundle,
        CostClass,
        LaunchMode,
        ProfileRegistry,
        ProjectConfig,
        ProjectRegistry,
        ProviderProfileConfig,
        ProviderRole,
        RuntimeConfiguration,
        SandboxProfileConfig,
        SandboxRegistry,
        TopicRegistry,
    )
    from vuzol.telegram.model_command import enqueue_worker_picker

    sandbox = SandboxProfileConfig.model_validate(
        {
            "id": "project-default",
            "image": "registry.example/vuzol-sandbox@sha256:" + ("0" * 64),
            "enabled": False,
        }
    )
    project = ProjectConfig.model_validate(
        {
            "id": "bill-buddy",
            "display_name": "Bill Buddy",
            "repository_path": "bill-buddy",
            "default_branch": "main",
            "allowed_capabilities": frozenset({Capability.REPOSITORY_READ}),
            "sandbox_profile": "project-default",
            "enabled": False,
        }
    )
    projects = ProjectRegistry((project,), repository_root=Path("/tmp"))  # noqa: S108
    profile = ProviderProfileConfig.model_validate(
        {
            "id": "codex-subscription-prod",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "launch_mode": LaunchMode.CLI,
            "credential_required": False,
            "capabilities": frozenset({Capability.REPOSITORY_READ}),
            "concurrency_limit": 1,
            "cost_class": CostClass.STRONG,
            "roles": frozenset({ProviderRole.EXECUTOR}),
            "routing_priority": 200,
            "supported_task_types": frozenset({"coding"}),
            "fallback_profile_ids": (),
            "sandbox_required": True,
            "runtime_identity": "vuzol-executor",
            "state_directory": Path("/var/lib/vuzol-provider-state/codex"),
            "enabled": True,
        }
    )
    registries = ConfigurationBundle(
        projects=projects,
        profiles=ProfileRegistry((profile,)),
        topics=TopicRegistry((), projects=projects),
        sandboxes=SandboxRegistry((sandbox,)),
        revision="test",
    )
    runtime = MagicMock(spec=RuntimeConfiguration)
    runtime.registries = registries

    class Row:
        project_id = "bill-buddy"
        mode = "auto"
        worker_key = None
        reasoning_effort = None
        revision = 1
        updated_by_user_id = None

    session = MagicMock()
    session.get = AsyncMock(return_value=Row())
    session.add = MagicMock()
    session.flush = AsyncMock()
    await enqueue_worker_picker(
        session,
        runtime=runtime,
        project_id="bill-buddy",
        chat_id=-100,
        message_thread_id=20,
        inbox_id=uuid4(),
    )
    assert session.add.called
    outbox = session.add.call_args.args[0]
    assert outbox.payload["role"] == PROJECT_MODEL_PICKER_ROLE
    assert outbox.payload["project_id"] == "bill-buddy"


@pytest.mark.anyio
async def test_controls_apply_model_action_without_workflow_outbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vuzol.telegram.controls import TelegramControlService
    from vuzol.telegram.domain import ControlUpdate

    update = ControlUpdate(
        bot_id="main",
        update_id=50,
        callback_query_id="cb-model-1",
        chat_id=-100,
        user_id=42,
        message_thread_id=20,
        action_kind="project_model_select_auto",
        preference_revision=1,
    )
    action_id = uuid4()
    workflow_enqueues: list[dict[str, object]] = []

    class FakeOutbox:
        async def enqueue(self, **kwargs: object) -> object:
            workflow_enqueues.append(kwargs)
            return uuid4()

    class FakeInbox:
        async def receive_once(self, **kwargs: object) -> tuple[object, bool]:
            del kwargs
            return uuid4(), True

        async def mark_processed(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    class FakeActions:
        async def queue_once(self, action: object) -> tuple[object, bool]:
            del action
            return action_id, True

    class FakeUow:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.inbox = FakeInbox()
            self.outbox = FakeOutbox()
            self.telegram_actions = FakeActions()
            self.session = MagicMock()

        async def __aenter__(self) -> "FakeUow":
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

    applied = AsyncMock(return_value=MagicMock(project_id="bill-buddy", stage="confirm"))
    monkeypatch.setattr("vuzol.telegram.controls.UnitOfWork", FakeUow)
    monkeypatch.setattr("vuzol.telegram.controls.authorize", lambda *a, **k: None)
    service = TelegramControlService(MagicMock(), MagicMock())
    service._project_model.apply = applied  # type: ignore[method-assign]
    result = await service.accept(update)
    assert result.status is IngressStatus.CREATED
    assert result.action_id == action_id
    applied.assert_awaited_once()
    assert workflow_enqueues == []
